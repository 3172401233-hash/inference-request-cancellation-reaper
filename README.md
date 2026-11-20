# inference-request-cancellation-reaper
清理取消推理后的资源、队列项和批量引用

面向 Node.js 24.16.0。当客户端断开后，推理请求可能仍停留在等待队列、动态批或结果缓存中。
`CancellationReaper` 根据请求当前所处的状态决定每种资源的释放时机，而不是简单地删除队列项。

## 状态机

```
queued ──admit──▶ batched ──startBatch──▶ running ──settleBatch──▶ completed
   │                 │                      │
   └──────cancel─────┴────────cancel────────┴──────cancel────────▶ cancelled
```

## 取消动作（`cancel()` 返回值）

| 当前状态    | 动作       | 行为                                                         |
| ----------- | ---------- | ------------------------------------------------------------ |
| `queued`    | `released` | 从队列移除，立即释放租约，请求终结                           |
| `batched`   | `deferred` | 只记录取消；GPU 批次不可中断，等 `settleBatch` 时才释放槽位  |
| `running`   | `deferred` | 同上，必须等待当前批次结束                                   |
| `completed` | `recorded` | 租约已在完成时释放，仅记录取消标志，不重复释放               |
| 重复/未知   | `ignored`  | 幂等，无副作用                                               |

## 资源释放边界

- **队列项**：仅在 `queued` 状态下取消时移除；一旦 `admit()` 进入批次，队列项已不存在。
- **租约 / GPU 槽位**：`batched` / `running` 状态下取消不会立即释放——批次仍在占用显存与算力，
  必须等 `settleBatch()` 回调时才释放。所有路径经过 `#releaseLease()` 的恰好一次守卫，
  取消与正常完成竞争时不会重复释放。
- **批次引用**：`settleBatch()` 一次性解散整个批次的成员表，取消的成员丢弃结果，
  未取消的成员转入 `completed` 并缓存结果。
- **结果缓存**：多路输出流共享同一请求时，结果只有在**全部**消费者 `close` 后才清除；
  任何一个消费者残留都会保住缓存与请求句柄。从未被消费的结果保留在缓存中，
  由调用方按需 `prune()` 回收。
- **请求句柄**：只有在「终态 + 租约已释放 + 无残留消费者 + 结果已清空」同时满足时才标记
  `settled`；已清理的请求保留墓碑记录，使迟到的取消能正确得到 `recorded` / `ignored`
  应答而非误报未知请求，`prune()` 负责最终移除墓碑。

## 用法

```js
const { CancellationReaper } = require('./src/reaper');

const reaper = new CancellationReaper({ queue });
const controller = new AbortController();

reaper.track(requestId, { lease, signal: controller.signal });
reaper.admit(requestId, batch);
reaper.startBatch(batch);
reaper.addOutput(requestId, res);        // 每路输出流注册一次
reaper.settleBatch(batch, { [requestId]: result });
```

事件：`lease:released`、`result:cleared`、`cancel:deferred`、`settled`。

## 测试

```sh
npm test    # node --test，覆盖批中取消、完成前取消、重复取消、输出流残留
```
