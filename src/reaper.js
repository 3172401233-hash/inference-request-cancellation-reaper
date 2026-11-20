'use strict';

const { EventEmitter } = require('node:events');

const State = Object.freeze({
  QUEUED: 'queued',
  BATCHED: 'batched',
  RUNNING: 'running',
  COMPLETED: 'completed',
  CANCELLED: 'cancelled',
});

const CancelAction = Object.freeze({
  RELEASED: 'released',
  DEFERRED: 'deferred',
  RECORDED: 'recorded',
  IGNORED: 'ignored',
});

class TrackedRequest {
  constructor(id, lease) {
    this.id = id;
    this.state = State.QUEUED;
    this.lease = lease ?? null;
    this.leaseReleased = this.lease === null;
    this.batch = null;
    this.cancelRequested = false;
    this.cancelReason = null;
    this.result = null;
    this.resultCached = false;
    this.consumers = new Set();
    this.settled = false;
  }
}

class CancellationReaper extends EventEmitter {
  constructor({ queue } = {}) {
    super();
    this.queue = queue ?? null;
    this.requests = new Map();
    this.batchMembers = new Map();
  }

  track(id, { lease, signal } = {}) {
    if (this.requests.has(id)) {
      throw new Error(`request already tracked: ${id}`);
    }
    const req = new TrackedRequest(id, lease);
    this.requests.set(id, req);
    if (signal) {
      if (signal.aborted) {
        this.cancel(id, signal.reason);
      } else {
        signal.addEventListener('abort', () => this.cancel(id, signal.reason), { once: true });
      }
    }
    return req;
  }

  admit(id, batch) {
    const req = this.#get(id);
    if (req.state !== State.QUEUED) return false;
    req.state = State.BATCHED;
    req.batch = batch;
    let members = this.batchMembers.get(batch);
    if (!members) {
      members = new Set();
      this.batchMembers.set(batch, members);
    }
    members.add(id);
    return true;
  }

  startBatch(batch) {
    const members = this.batchMembers.get(batch);
    if (!members) return;
    for (const id of members) {
      const req = this.requests.get(id);
      if (req && req.state === State.BATCHED) req.state = State.RUNNING;
    }
  }

  settleBatch(batch, results = {}) {
    const members = this.batchMembers.get(batch);
    if (!members) return;
    this.batchMembers.delete(batch);
    for (const id of members) {
      const req = this.requests.get(id);
      if (!req) continue;
      if (req.cancelRequested) {
        req.state = State.CANCELLED;
        req.result = null;
        req.resultCached = false;
      } else {
        req.state = State.COMPLETED;
        req.result = results instanceof Map ? results.get(id) : results[id];
        req.resultCached = true;
      }
      this.#releaseLease(req);
      this.#tryReap(req);
    }
  }

  cancel(id, reason = null) {
    const req = this.requests.get(id);
    if (!req || req.cancelRequested) {
      return { state: req ? req.state : null, action: CancelAction.IGNORED };
    }
    req.cancelRequested = true;
    req.cancelReason = reason;
    switch (req.state) {
      case State.QUEUED: {
        req.state = State.CANCELLED;
        if (this.queue && typeof this.queue.remove === 'function') {
          this.queue.remove(id);
        }
        this.#releaseLease(req);
        this.#tryReap(req);
        return { state: req.state, action: CancelAction.RELEASED };
      }
      case State.BATCHED:
      case State.RUNNING: {
        req.state = State.CANCELLED;
        this.emit('cancel:deferred', id);
        return { state: req.state, action: CancelAction.DEFERRED };
      }
      case State.COMPLETED: {
        return { state: req.state, action: CancelAction.RECORDED };
      }
      default: {
        return { state: req.state, action: CancelAction.IGNORED };
      }
    }
  }

  addOutput(id, stream) {
    const req = this.#get(id);
    if (req.consumers.has(stream)) return () => {};
    req.consumers.add(stream);
    const detach = () => {
      if (req.consumers.delete(stream)) {
        if (req.consumers.size === 0) this.#clearResult(req);
        this.#tryReap(req);
      }
    };
    if (typeof stream.once === 'function') {
      stream.once('close', detach);
    } else if (typeof stream.addEventListener === 'function') {
      stream.addEventListener('close', detach, { once: true });
    }
    return detach;
  }

  getState(id) {
    const req = this.requests.get(id);
    return req ? req.state : null;
  }

  resultFor(id) {
    const req = this.requests.get(id);
    return req && req.resultCached ? req.result : undefined;
  }

  isSettled(id) {
    const req = this.requests.get(id);
    return req ? req.settled : true;
  }

  prune() {
    let removed = 0;
    for (const [id, req] of this.requests) {
      if (req.settled) {
        this.requests.delete(id);
        removed += 1;
      }
    }
    return removed;
  }

  #get(id) {
    const req = this.requests.get(id);
    if (!req) throw new Error(`unknown request: ${id}`);
    return req;
  }

  #releaseLease(req) {
    if (req.leaseReleased) return;
    req.leaseReleased = true;
    if (req.lease) req.lease.release();
    this.emit('lease:released', req.id);
  }

  #clearResult(req) {
    if (!req.resultCached) return;
    req.result = null;
    req.resultCached = false;
    this.emit('result:cleared', req.id);
  }

  #tryReap(req) {
    const terminal = req.state === State.COMPLETED || req.state === State.CANCELLED;
    if (!terminal || !req.leaseReleased || req.consumers.size > 0 || req.resultCached) return;
    if (!req.settled) {
      req.settled = true;
      this.emit('settled', req.id, req.state);
    }
  }
}

module.exports = { CancellationReaper, State, CancelAction };
