'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');

const { CancellationReaper, State, CancelAction } = require('../src/reaper');

function makeLease() {
  return {
    releases: 0,
    release() {
      this.releases += 1;
    },
  };
}

function makeQueue() {
  return {
    removed: [],
    remove(id) {
      this.removed.push(id);
    },
  };
}

function makeStream() {
  const stream = new EventEmitter();
  stream.close = () => stream.emit('close');
  return stream;
}

test('cancel while queued removes queue item and releases lease immediately', () => {
  const queue = makeQueue();
  const reaper = new CancellationReaper({ queue });
  const lease = makeLease();
  reaper.track('r1', { lease });

  const outcome = reaper.cancel('r1', 'client disconnected');

  assert.equal(outcome.action, CancelAction.RELEASED);
  assert.equal(outcome.state, State.CANCELLED);
  assert.deepEqual(queue.removed, ['r1']);
  assert.equal(lease.releases, 1);
  assert.equal(reaper.isSettled('r1'), true);
});

test('cancel inside a batch defers release until the batch settles', () => {
  const reaper = new CancellationReaper();
  const leaseA = makeLease();
  const leaseB = makeLease();
  const batch = { id: 'batch-1' };

  reaper.track('a', { lease: leaseA });
  reaper.track('b', { lease: leaseB });
  reaper.admit('a', batch);
  reaper.admit('b', batch);
  reaper.startBatch(batch);

  const outcome = reaper.cancel('a');

  assert.equal(outcome.action, CancelAction.DEFERRED);
  assert.equal(reaper.getState('a'), State.CANCELLED);
  assert.equal(leaseA.releases, 0, 'GPU batch still holds the slot');
  assert.equal(reaper.isSettled('a'), false);

  reaper.settleBatch(batch, { a: 'discarded', b: 'kept' });

  assert.equal(leaseA.releases, 1);
  assert.equal(leaseB.releases, 1);
  assert.equal(reaper.resultFor('a'), undefined, 'cancelled result is dropped');
  assert.equal(reaper.resultFor('b'), 'kept');
  assert.equal(reaper.isSettled('a'), true);
});

test('cancel before completion wins the race and releases exactly once', () => {
  const reaper = new CancellationReaper();
  const lease = makeLease();
  const batch = { id: 'batch-2' };

  reaper.track('r', { lease });
  reaper.admit('r', batch);
  reaper.startBatch(batch);
  reaper.cancel('r');
  reaper.settleBatch(batch, { r: 'late result' });
  reaper.cancel('r');

  assert.equal(lease.releases, 1, 'no double release across the race');
  assert.equal(reaper.resultFor('r'), undefined);
  assert.equal(reaper.isSettled('r'), true);
});

test('duplicate cancellation is ignored and never releases twice', () => {
  const queue = makeQueue();
  const reaper = new CancellationReaper({ queue });
  const lease = makeLease();
  reaper.track('r', { lease });

  const first = reaper.cancel('r');
  const second = reaper.cancel('r');
  const third = reaper.cancel('missing');

  assert.equal(first.action, CancelAction.RELEASED);
  assert.equal(second.action, CancelAction.IGNORED);
  assert.equal(third.action, CancelAction.IGNORED);
  assert.equal(lease.releases, 1);
  assert.deepEqual(queue.removed, ['r']);
});

test('cancel after completion is only recorded, resources stay released once', () => {
  const reaper = new CancellationReaper();
  const lease = makeLease();
  const batch = { id: 'batch-3' };

  reaper.track('r', { lease });
  reaper.admit('r', batch);
  reaper.startBatch(batch);
  reaper.settleBatch(batch, { r: 'done' });

  assert.equal(lease.releases, 1);

  const outcome = reaper.cancel('r');

  assert.equal(outcome.action, CancelAction.RECORDED);
  assert.equal(lease.releases, 1, 'completion already released the lease');
});

test('result cache survives until every output stream closes', () => {
  const reaper = new CancellationReaper();
  const batch = { id: 'batch-4' };
  const streamA = makeStream();
  const streamB = makeStream();
  const cleared = [];
  reaper.on('result:cleared', (id) => cleared.push(id));

  reaper.track('r', { lease: makeLease() });
  reaper.admit('r', batch);
  reaper.startBatch(batch);
  reaper.addOutput('r', streamA);
  reaper.addOutput('r', streamB);
  reaper.settleBatch(batch, { r: 'shared result' });

  assert.equal(reaper.resultFor('r'), 'shared result');
  assert.equal(reaper.isSettled('r'), false);

  streamA.close();
  assert.equal(reaper.resultFor('r'), 'shared result', 'one consumer still reading');
  assert.equal(reaper.isSettled('r'), false);

  streamB.close();
  assert.equal(reaper.resultFor('r'), undefined);
  assert.deepEqual(cleared, ['r']);
  assert.equal(reaper.isSettled('r'), true);
});

test('cancelled request with lingering streams reaps only after consumers close', () => {
  const reaper = new CancellationReaper();
  const lease = makeLease();
  const batch = { id: 'batch-5' };
  const stream = makeStream();

  reaper.track('r', { lease });
  reaper.admit('r', batch);
  reaper.startBatch(batch);
  reaper.addOutput('r', stream);
  reaper.cancel('r');
  reaper.settleBatch(batch, { r: 'discarded' });

  assert.equal(lease.releases, 1, 'slot freed when batch finished');
  assert.equal(reaper.isSettled('r'), false, 'stream listener must not leak');

  stream.close();
  assert.equal(reaper.isSettled('r'), true);
});

test('AbortSignal drives cancellation through track()', () => {
  const queue = makeQueue();
  const reaper = new CancellationReaper({ queue });
  const controller = new AbortController();
  const lease = makeLease();

  reaper.track('r', { lease, signal: controller.signal });
  controller.abort(new Error('client gone'));

  assert.equal(reaper.getState('r'), State.CANCELLED);
  assert.equal(reaper.isSettled('r'), true);
  assert.equal(lease.releases, 1);
  assert.deepEqual(queue.removed, ['r']);
  assert.equal(reaper.prune(), 1);
  assert.equal(reaper.getState('r'), null, 'pruned requests leave the registry');
});

test('cancelled queued request cannot be admitted into a later batch', () => {
  const reaper = new CancellationReaper();
  reaper.track('r', { lease: makeLease() });
  reaper.cancel('r');

  assert.equal(reaper.admit('r', { id: 'batch-6' }), false);
});
