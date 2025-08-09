/**
 * @fileoverview
 * This file contains the MongoCheckpointSaver class, which is used to save checkpoints to a MongoDB database.
 * It implements the BaseCheckpointSaver interface from LangGraph.
 *
 * @author Muhammad Umer Farooq <umer@lablnet.com>
 * @since v1.0.0
 */

import type {
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple
  } from '@langchain/langgraph'
  import { BaseCheckpointSaver } from '@langchain/langgraph'
  import type { RunnableConfig } from '@langchain/core/runnables'
  import { MongoClient, Db, Collection, ObjectId } from 'mongodb'
  import { Buffer } from 'node:buffer'
  
  // Define PendingWrite type locally since it's not exported from @langchain/langgraph
  type PendingWrite<Channel = string> = [Channel, unknown]
  
  // Define ChannelVersions type locally since it's not exported from @langchain/langgraph
  type ChannelVersion = number | string
  type ChannelVersions = Record<string, ChannelVersion>
  
  /**
   * @description
   * This type represents a checkpoint row in the database.
   */
  type CheckpointRow = {
    thread_id: string
    checkpoint_ns: string
    checkpoint_id: string
    ts: Date
    // New serialized fields
    checkpoint_type?: string
    checkpoint_data?: string // base64-encoded bytes
    metadata_type?: string
    metadata_data?: string // base64-encoded bytes
    // Backwards compatibility with older rows
    checkpoint?: Checkpoint
  }
  
  /**
   * @description
   * This type represents a write row in the database.
   */
  type WriteRow = {
    thread_id: string
    checkpoint_ns: string
    checkpoint_id: string
    ts: Date
    order: number
    // New serialized fields
    write_type?: string
    write_data?: string // base64-encoded bytes
    channel?: string
    task_id?: string
    // Backwards compatibility with older rows
    write?: PendingWrite
    // Some code paths also used idx for ordering; keep optional for compat
    idx?: number
  }
  
  /**
   * @description
   * This type represents a version row in the database.
   */
  type VersionRow = {
    thread_id: string
    checkpoint_ns: string
    version: number
  }
  
  /**
   * @description
   * This class is used to save checkpoints to a MongoDB database.
   * It implements the BaseCheckpointSaver interface from LangGraph.
   *
   * @param client - The MongoDB client.
   * @param dbName - The name of the database to use.
   * @since v1.0.0
   * @author Muhammad Umer Farooq <umer@lablnet.com>
   */
  export class MongoCheckpointSaver extends BaseCheckpointSaver {
    private db: Db
    private checkpoints: Collection<CheckpointRow>
    private writes: Collection<WriteRow>
    private versions: Collection<VersionRow>
  
    /**
     *
     * @description
     * This class is used to save checkpoints to a MongoDB database.
     * It implements the BaseCheckpointSaver interface from LangGraph.
     *
     * @param client - The MongoDB client.
     * @param dbName - The name of the database to use.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    constructor (client: MongoClient, dbName?: string, serde?: any) {
      super(serde)
      this.db = dbName ? client.db(dbName) : client.db()
      this.checkpoints = this.db.collection('checkpoints')
      this.writes = this.db.collection('checkpoint_writes')
      this.versions = this.db.collection<VersionRow>('checkpoint_versions')
  
      // index for checkpoint lookup
      this.checkpoints.createIndex(
        { thread_id: 1, checkpoint_ns: 1, checkpoint_id: 1 },
        { unique: true }
      )
      // index for checkpoint lookup by timestamp
      this.checkpoints.createIndex({ thread_id: 1, checkpoint_ns: 1, ts: -1 })
  
      // index for writes lookup
      this.writes.createIndex({
        thread_id: 1,
        checkpoint_ns: 1,
        checkpoint_id: 1
      })
      this.writes.createIndex({ thread_id: 1, checkpoint_ns: 1, ts: -1 })
  
      // Version counter per (thread, ns)
      this.versions.createIndex(
        { thread_id: 1, checkpoint_ns: 1 },
        { unique: true }
      )
    }
  
    private isValidCheckpoint(candidate: any): candidate is Checkpoint {
      return (
        candidate &&
        typeof candidate === 'object' &&
        typeof candidate.v !== 'undefined' &&
        typeof candidate.id === 'string' &&
        typeof candidate.ts === 'string' &&
        typeof candidate.channel_values === 'object' &&
        typeof candidate.channel_versions === 'object' &&
        typeof candidate.versions_seen === 'object' &&
        Array.isArray(candidate.pending_sends)
      )
    }
  
    /**
     * Store a checkpoint snapshot. Must return a RunnableConfig that
     * points to this exact checkpoint (so LangGraph can resume).
     *
     * @param config - The config object containing the thread_id and checkpoint_ns.
     * @param checkpoint - The checkpoint to store.
     * @param metadata - The metadata to store.
     * @param newVersions - The new versions to store.
     * @returns The config object containing the thread_id and checkpoint_ns.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    async put (
      config: RunnableConfig,
      checkpoint: Checkpoint,
      metadata: CheckpointMetadata,
      newVersions: ChannelVersions
    ): Promise<RunnableConfig> {
      const thread_id = config.configurable?.thread_id
      const checkpoint_ns = config.configurable?.checkpoint_ns ?? ''
      if (thread_id === undefined || checkpoint_ns === undefined) {
        throw new Error(
          `The provided config must contain a configurable field with "thread_id" and "checkpoint_ns" fields.`
        )
      }
      const checkpoint_id = checkpoint.id ?? new ObjectId().toHexString()
  
      const row: CheckpointRow = {
        thread_id,
        checkpoint_ns,
        checkpoint_id,
        ts: new Date(checkpoint.ts ?? new Date().toISOString())
      }
  
      // Serialize checkpoint + metadata using LangGraph serde
      try {
        const [[cType, cBytes], [mType, mBytes]] = await Promise.all([
          this.serde.dumpsTyped(checkpoint),
          this.serde.dumpsTyped(metadata),
        ])
        if (cType !== mType) {
          throw new Error('Mismatched checkpoint and metadata types')
        }
        row.checkpoint_type = cType
        row.checkpoint_data = Buffer.from(cBytes).toString('base64')
        row.metadata_type = mType
        row.metadata_data = Buffer.from(mBytes).toString('base64')
        // Do not store legacy field when using typed storage
      } catch (_e) {
        // Fallback: store raw (not recommended, but keeps system usable)
        ;(row as any).checkpoint = checkpoint
      }
  
      await this.checkpoints.updateOne(
        { thread_id, checkpoint_ns, checkpoint_id },
        { $set: row },
        { upsert: true }
      )
  
      // Return a config that references this checkpoint id explicitly.
      return {
        ...config,
        configurable: {
          ...(config.configurable ?? {}),
          thread_id,
          checkpoint_ns,
          checkpoint_id
        }
      }
    }
  
    /**
     *
     * @param config - The config object containing the thread_id and checkpoint_ns.
     * @param writes - The writes to store.
     * @param checkpoint - The checkpoint to store.
     * @returns The number of documents inserted.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    async putWrites (
      config: RunnableConfig,
      writes: PendingWrite[],
      taskId: string
    ): Promise<void> {
      const thread_id = config.configurable?.thread_id
      const checkpoint_ns = config.configurable?.checkpoint_ns
      const checkpoint_id = (config.configurable as any)?.checkpoint_id
      if (thread_id === undefined || checkpoint_ns === undefined) {
        throw new Error(
          `The provided config must contain a configurable field with "thread_id" and "checkpoint_ns" fields.`
        )
      }
      const ts = new Date()
  
      if (writes?.length) {
        const docs: WriteRow[] = await Promise.all(writes.map(async (w, i) => {
          const [channel, value] = w as [string, unknown]
          const doc: WriteRow = {
            thread_id,
            checkpoint_ns,
            checkpoint_id,
            ts,
            idx: i,
            order: i,
            channel,
            task_id: taskId
          }
          try {
            const [type, bytes] = await this.serde.dumpsTyped(value)
            doc.write_type = type
            doc.write_data = Buffer.from(bytes).toString('base64')
          } catch (_e) {
            // Fallback for legacy
            doc.write = w
          }
          return doc
        }))
        await this.writes.insertMany(docs as any)
      }
    }
  
    /**
     * Retrieves the latest checkpoint for a given thread and checkpoint namespace.
     *
     * @param config - The config object containing the thread_id and checkpoint_ns.
     * @returns The checkpoint.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    async get (config: RunnableConfig): Promise<Checkpoint | undefined> {
      const { thread_id, checkpoint_ns = '' } = (config.configurable ?? {}) as any
      if (!thread_id) return undefined
  
      const row = await this.checkpoints
        .find({ thread_id, checkpoint_ns })
        .sort({ ts: -1 })
        .limit(1)
        .next()
  
      if (!row) return undefined
      // Deserialize typed checkpoint if present
      if ((row as any).checkpoint_type && (row as any).checkpoint_data) {
        const type = (row as any).checkpoint_type as string
        const dataB64 = (row as any).checkpoint_data as string
        const bytes = Buffer.from(dataB64, 'base64')
        const loaded = await this.serde.loadsTyped(type, new Uint8Array(bytes)) as any
        if (this.isValidCheckpoint(loaded)) {
          return loaded as Checkpoint
        }
        // If typed load failed to produce a proper checkpoint, try legacy field
        const legacy = (row as any).checkpoint
        if (this.isValidCheckpoint(legacy)) return legacy as Checkpoint
        return undefined
      }
      // Backwards compatibility: raw checkpoint
      const legacy = (row as any).checkpoint
      if (this.isValidCheckpoint(legacy)) return legacy as Checkpoint
      return undefined
    }
  
    /**
     * Retrieves a checkpoint tuple from the database.
     *
     * @param config - The config object containing the thread_id and checkpoint_ns.
     * @returns The checkpoint tuple.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    async getTuple (config: RunnableConfig): Promise<CheckpointTuple | undefined> {
      const thread_id = config.configurable?.thread_id
      const checkpoint_ns = config.configurable?.checkpoint_ns
  
      // get the latest checkpoint
      const row: any = await this.checkpoints
        .find({ thread_id, checkpoint_ns })
        .sort({ ts: -1 })
        .limit(1)
        .next()
      if (!row) return undefined
  
      // get the writes for the checkpoint
      const writeRows = await this.writes
        .find({ thread_id, checkpoint_ns, checkpoint_id: row.checkpoint_id })
        .sort({ idx: 1 })
        .toArray()
  
      // get the pending writes for the checkpoint
      const pendingWrites: any[] = await Promise.all(writeRows.map(async (w: any) => {
        let value: any
        if (w.write_type && w.write_data) {
          const bytes = Buffer.from(w.write_data, 'base64')
          value = await this.serde.loadsTyped(w.write_type, new Uint8Array(bytes))
        } else {
          value = (w.write as PendingWrite)?.[1]
        }
        // Shape must be [taskId, channel, value]
        return [w.task_id ?? w.checkpoint_id, w.channel ?? (Array.isArray(w.write) ? w.write[0] : undefined), value]
      }))
  
      // Deserialize checkpoint
      let checkpoint: Checkpoint
      if ((row as any).checkpoint_type && (row as any).checkpoint_data) {
        const bytes = Buffer.from((row as any).checkpoint_data, 'base64')
        const loaded = await this.serde.loadsTyped(
          (row as any).checkpoint_type,
          new Uint8Array(bytes)
        )
        if (this.isValidCheckpoint(loaded)) {
          checkpoint = loaded as Checkpoint
        } else if (this.isValidCheckpoint((row as any).checkpoint)) {
          checkpoint = (row as any).checkpoint as Checkpoint
        } else {
          // Invalid checkpoint, treat as no checkpoint
          return undefined
        }
      } else {
        if (this.isValidCheckpoint((row as any).checkpoint)) {
          checkpoint = (row as any).checkpoint as Checkpoint
        } else {
          return undefined
        }
      }
  
      // Deserialize metadata if present
      let metadata: any | undefined
      if ((row as any).metadata_type && (row as any).metadata_data) {
        const mb = Buffer.from((row as any).metadata_data, 'base64')
        metadata = await this.serde.loadsTyped((row as any).metadata_type, new Uint8Array(mb))
      }
  
      // return the checkpoint tuple
      return {
        config: {
          configurable: {
            ...(config.configurable ?? {}),
            thread_id,
            checkpoint_ns,
            checkpoint_id: row.checkpoint_id,
          }
        },
        checkpoint,
        metadata,
        parentConfig: undefined,
        pendingWrites
      }
    }
  
    /**
     * Retrieves a list of checkpoints from the database.
     *
     * @param config - The config object containing the thread_id and checkpoint_ns.
     * @param options - The options object containing the limit, before, and filter.
     * @returns The list of checkpoints.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    async *list (
      config: RunnableConfig,
      options: {
        limit?: number
        before?: RunnableConfig
        filter?: Record<string, any>
      } = {}
    ) {
      const thread_id = config.configurable?.thread_id
      const checkpoint_ns = config.configurable?.checkpoint_ns ?? ''
  
      if (!thread_id) return
  
      const query: any = {
        thread_id,
        checkpoint_ns,
        ...(options.filter ?? {})
      }
  
      // If "before" is provided, only return checkpoints older than it
      if (options.before?.configurable?.checkpoint_id) {
        const beforeCheckpoint = await this.checkpoints.findOne({
          thread_id,
          checkpoint_ns,
          checkpoint_id: options.before.configurable.checkpoint_id
        })
        if (beforeCheckpoint) {
          query.ts = { $lt: beforeCheckpoint.ts }
        }
      }
  
      const cursor = this.checkpoints
        .find(query)
        .sort({ ts: -1 })
        .limit(options.limit ?? 50)
  
      for await (const row of cursor) {
        // Deserialize checkpoint for each row
        let checkpoint: Checkpoint
        if ((row as any).checkpoint_type && (row as any).checkpoint_data) {
          const bytes = Buffer.from((row as any).checkpoint_data, 'base64')
          checkpoint = await this.serde.loadsTyped(
            (row as any).checkpoint_type,
            new Uint8Array(bytes)
          ) as Checkpoint
        } else {
          checkpoint = (row as any).checkpoint as Checkpoint
        }
  
        yield {
          config: {
            configurable: {
              thread_id,
              checkpoint_ns,
              checkpoint_id: row.checkpoint_id
            }
          },
          checkpoint,
          metadata: undefined
        }
      }
    }
  
    /**
     * This method is used to get the next version for a channel.
     *
     * @param current - The current version.
     * @param _channel - The channel.
     * @returns The next version.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    getNextVersion (current: number | undefined, _channel?: any): number {
      if (current === undefined) return 2
      return current + 2
    }
  
    /**
     * Clears all checkpoints and writes for a given thread and checkpoint namespace.
     *
     * @param thread_id - The ID of the thread to clear.
     * @param checkpoint_ns - The namespace of the checkpoint to clear.
     * @returns The number of documents deleted.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    async deleteThread (thread_id: string, checkpoint_ns = '') {
      await Promise.all([
        this.checkpoints.deleteMany({ thread_id, checkpoint_ns }),
        this.writes.deleteMany({ thread_id, checkpoint_ns }),
        this.versions.deleteMany({ thread_id, checkpoint_ns })
      ])
    }
  
    /**
     * Clears all checkpoints, writes, and versions from the database.
     *
     * @returns The number of documents deleted.
     * @since v1.0.0
     * @author Muhammad Umer Farooq <umer@lablnet.com>
     */
    async clearAll () {
      await Promise.all([
        this.checkpoints.deleteMany({}),
        this.writes.deleteMany({}),
        this.versions.deleteMany({})
      ])
    }
  }
