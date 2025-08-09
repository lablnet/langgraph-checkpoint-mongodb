import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoMemoryServer } from 'mongodb-memory-server'
import { MongoClient } from 'mongodb'
import { MongoCheckpointSaver } from '../src/MongoCheckpointSaver'

// Minimal JSON-based serde compatible with the saver
const jsonSerde = {
  async dumpsTyped(value: unknown): Promise<[string, Uint8Array]> {
    const buf = Buffer.from(JSON.stringify(value))
    return ['json', new Uint8Array(buf)]
  },
  async loadsTyped(_type: string, bytes: Uint8Array): Promise<unknown> {
    const str = Buffer.from(bytes).toString('utf-8')
    return JSON.parse(str)
  },
}

describe('MongoCheckpointSaver', () => {
  let mongod: MongoMemoryServer
  let client: MongoClient

  beforeAll(async () => {
    mongod = await MongoMemoryServer.create()
    const uri = mongod.getUri()
    client = await MongoClient.connect(uri)
  })

  afterAll(async () => {
    await client?.close()
    await mongod?.stop()
  })

  it('should put and get a checkpoint', async () => {
    const saver = new MongoCheckpointSaver(client, 'testdb', jsonSerde)
    const config = { configurable: { thread_id: 't1', checkpoint_ns: 'default' } }

    const checkpoint = {
      v: 1,
      id: 'cp-1',
      ts: new Date().toISOString(),
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
      pending_sends: [],
    }
    const metadata = { note: 'meta' } as any
    const newVersions = {}

    const retCfg = await saver.put(config as any, checkpoint as any, metadata, newVersions)
    expect(retCfg.configurable?.checkpoint_id).toBeDefined()

    const got = await saver.get(config as any)
    expect(got?.id).toBe('cp-1')
  })

  it('should write pending writes and read tuple', async () => {
    const saver = new MongoCheckpointSaver(client, 'testdb', jsonSerde)
    const config = { configurable: { thread_id: 't2', checkpoint_ns: 'ns1' } }

    const checkpoint = {
      v: 1,
      id: 'cp-2',
      ts: new Date().toISOString(),
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
      pending_sends: [],
    }
    const savedCfg = await saver.put(config as any, checkpoint as any, { x: 1 } as any, {})

    await saver.putWrites(savedCfg as any, [['alpha', { a: 1 }], ['beta', { b: 2 }]] as any, 'task-1')

    const tuple = await saver.getTuple(config as any)
    expect(tuple?.checkpoint.id).toBe('cp-2')
    expect(tuple?.pendingWrites?.length).toBe(2)
    expect(tuple?.pendingWrites?.[0][0]).toBe('task-1')
  })

  it('should list checkpoints', async () => {
    const saver = new MongoCheckpointSaver(client, 'testdb', jsonSerde)
    const config = { configurable: { thread_id: 't3', checkpoint_ns: 'ns2' } }
    for (let i = 0; i < 3; i++) {
      const cp = {
        v: 1,
        id: `cp-${i}`,
        ts: new Date().toISOString(),
        channel_values: {},
        channel_versions: {},
        versions_seen: {},
        pending_sends: [],
      }
      await saver.put(config as any, cp as any, { source: 'input', step: 0, parents: {} } as any, {})
    }
    const items: any[] = []
    for await (const item of saver.list(config as any)) {
      items.push(item)
    }
    expect(items.length).toBeGreaterThanOrEqual(3)
  })

  it('should delete thread data and clear all', async () => {
    const saver = new MongoCheckpointSaver(client, 'testdb', jsonSerde)
    const config = { configurable: { thread_id: 't4', checkpoint_ns: 'ns3' } }
    await saver.put(config as any, {
      v: 1,
      id: 'cp-x',
      ts: new Date().toISOString(),
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
      pending_sends: [],
    } as any, { source: 'input', step: 0, parents: {} } as any, {})

    // delete specific thread/ns
    await saver.deleteThread('t4', 'ns3')
    const got = await saver.get(config as any)
    expect(got).toBeUndefined()

    // insert another and then clear all
    const cfg2 = { configurable: { thread_id: 't5', checkpoint_ns: 'ns4' } }
    await saver.put(cfg2 as any, {
      v: 1,
      id: 'cp-y',
      ts: new Date().toISOString(),
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
      pending_sends: [],
    } as any, { source: 'input', step: 0, parents: {} } as any, {})
    await saver.clearAll()
    const got2 = await saver.get(cfg2 as any)
    expect(got2).toBeUndefined()
  })
})
