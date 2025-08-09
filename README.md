### langgraph-checkpoint-mongodb

Production‑ready MongoDB checkpointer for LangGraph. A lightweight implementation of `BaseCheckpointSaver` that persists checkpoints and writes in MongoDB. Works out‑of‑the‑box with LangGraph’s serde: pass a serde for efficient typed storage, or omit it to store legacy shapes for compatibility. Ships with ESM/CJS builds and TypeScript types.

### Installation

```bash
pnpm add langgraph-checkpoint-mongodb
# or: npm i langgraph-checkpoint-mongodb
```

Peer deps you likely already have:
- `@langchain/langgraph`
- `@langchain/core`
- `mongodb`

### Usage

TypeScript (ESM) — simple usage (no serde):
```ts
import { MongoClient } from 'mongodb'
import type { BaseCheckpointSaver } from '@langchain/langgraph'
import { MongoCheckpointSaver } from 'langgraph-checkpoint-mongodb'

let saver: BaseCheckpointSaver | null = null

export async function getCheckpointer(): Promise<BaseCheckpointSaver> {
  if (saver) return saver
  const dbName = process.env.MONGODB_DB!
  const uri = process.env.MONGODB_URI!
  const client = await MongoClient.connect(uri)
  saver = new MongoCheckpointSaver(client, dbName)
  return saver
}

export async function closeCheckpointer() {
  if (!saver) return
  // if you kept a client reference, close it here
  saver = null
}
```

TypeScript (ESM) — with a custom serde:
```js
import { MongoClient } from 'mongodb'
import { MongoCheckpointSaver } from 'langgraph-checkpoint-mongodb'

const serde = {
  async dumpsTyped(v) {
    const buf = Buffer.from(JSON.stringify(v))
    return ['json', new Uint8Array(buf)] as const
  },
  async loadsTyped(_t, b) {
    return JSON.parse(Buffer.from(b).toString('utf8'))
  },
}

const client = await MongoClient.connect(process.env.MONGODB_URI!)
const saver = new MongoCheckpointSaver(client, process.env.MONGODB_DB!, serde)
// provide `saver` to your LangGraph app
```

JavaScript (CJS) — minimal:
```js
const { MongoClient } = require('mongodb')
const { MongoCheckpointSaver } = require('langgraph-checkpoint-mongodb')

(async () => {
  const client = await MongoClient.connect(process.env.MONGODB_URI)
  const saver = new MongoCheckpointSaver(client, process.env.MONGODB_DB)
  // use saver
})()
```

### API

- `new MongoCheckpointSaver(client: MongoClient, dbName?: string, serde?: any)`
  - Uses collections: `checkpoints`, `checkpoint_writes`, `checkpoint_versions`
- `put(config, checkpoint, metadata, newVersions)` → stores a checkpoint
- `putWrites(config, writes, taskId)` → stores pending writes
- `get(config)` → latest checkpoint
- `getTuple(config)` → `CheckpointTuple` with `pendingWrites`
- `list(config, options)` → async iterator of checkpoints
- `deleteThread(thread_id, checkpoint_ns?)` → clears a thread namespace
- `clearAll()` → wipes all collections

Notes:
- Provide the LangGraph serde you use in your app; this saver persists typed bytes and falls back to legacy shape for compatibility.
- Ensure your `RunnableConfig` has `configurable.thread_id` and `configurable.checkpoint_ns`.

### Development

Build:
```bash
pnpm run build
```

Test:
```bash
pnpm i
pnpm run test
```
