import {
  RedisClientOptions,
  createClient,
  RediSearchSchema,
  SchemaFieldTypes,
  SearchOptions,
} from "redis";
import { ListKeyOptions, RecordManager, UpdateOptions } from "./base.js";

export interface RedisRecordManagerOptions {
  /**
   * Optional index name to use for the RediSearch index.
   */
  indexName?: string;
  /**
   * Options to create the Redis client.
   * Either this or `redisClient` must be provided.
   */
  redisClientOptions?: RedisClientOptions;
  /**
   * An existing Redis client to use.
   * Either this or `redisClientOptions` must be provided.
   */
  redisClient?: ReturnType<typeof createClient>;
}

type QueryOptions = ListKeyOptions & { limit: number; offset: number };

export class RedisRecordManager extends RecordManager {
  private searchBatchSize = 1000;

  namespace: string;

  client: ReturnType<typeof createClient>;

  indexName = "langchain_record_index";

  /**
   * Creates a new RedisRecordManager.
   * @param {string} namespace The namespace to use for the keys.
   * @param {RedisRecordManagerOptions} config The configuration options for the record manager.
   * @throws If neither `redisClientOptions` nor `redisClient` is provided.
   *
   * If both `redisClientOptions` and `redisClient` are provided, the existing client will be used.
   */
  constructor(namespace: string, config: RedisRecordManagerOptions) {
    super();
    const { indexName, redisClientOptions, redisClient } = config;
    if (!redisClientOptions && !redisClient) {
      throw new Error(
        "Either redisClientOptions or redisClient must be provided."
      );
    }
    this.namespace = namespace;
    this.indexName = indexName ?? this.indexName;
    this.client = redisClient ?? createClient(redisClientOptions);
  }

  /**
   * Sets up the record manager by connecting to the Redis server and creating the index.
   */
  async createSchema(): Promise<void> {
    await this.connectClient();
    await this.createIndex();
  }

  async getTime(): Promise<number> {
    const time = await this.client.TIME();
    return time.getTime() + (time.microseconds % 1000) / 1000;
  }

  async update(
    keys: string[],
    updateOptions?: UpdateOptions | undefined
  ): Promise<void> {
    if (keys.length === 0) {
      return;
    }

    const { timeAtLeast, groupIds: _groupIds } = updateOptions ?? {};

    const updatedAt = await this.getTime();

    if (timeAtLeast && updatedAt < timeAtLeast) {
      throw new Error(
        `Time sync issue with database ${updatedAt} < ${timeAtLeast}`
      );
    }

    const groupIds = (_groupIds ?? keys.map(() => undefined)).map((groupId) =>
      groupId === null ? undefined : groupId
    );

    const command = keys.reduce((command, key, idx) => {
      const insertObject: Record<string, string | number> = {
        updatedAt,
      };

      if (groupIds[idx]) {
        insertObject.groupId = groupIds[idx] as string;
      }

      return command.hSet(`${this.namespace}:${key}`, insertObject);
    }, this.client.multi());

    await command.exec();
  }

  async exists(keys: string[]): Promise<boolean[]> {
    if (keys.length === 0) {
      return [];
    }

    const command = keys.reduce(
      (command, key) => command.exists(`${this.namespace}:${key}`),
      this.client.multi()
    );

    const exists = await command.exec();
    return exists.map((result) => result === 1);
  }

  async listKeys(options?: ListKeyOptions): Promise<string[]> {
    let hasMore = true;
    const { limit: initialLimit = Number.POSITIVE_INFINITY } = options ?? {};
    const results: string[] = [];

    // Redis search has a configurable limit of search results
    // so we need to paginate through the results if there are more than that limit.
    // If no limit is provided to the function, we default to infinity. This just means that 'hasMore'
    // will only be false when there are no more results. This is not atomic, but should
    // be fine for most reasonable use cases.
    while (hasMore) {
      const currentLimit = initialLimit - results.length;
      const result = await this.client.ft.search(
        this.indexName,
        ...this.buildSearchQuery({
          ...options,
          limit: currentLimit,
          offset: results.length,
        })
      );
      const numDocs = result.documents.length;
      // there are still results returned, and we have not reached the limit
      hasMore = numDocs !== 0 && numDocs < currentLimit;
      results.push(...this.extractKeys(result.documents));
    }

    return results;
  }

  async deleteKeys(keys: string[]): Promise<void> {
    if (keys.length === 0) {
      return;
    }

    const command = this.client.multi();
    await command.del(keys.map((key) => `${this.namespace}:${key}`)).exec();
  }

  /**
   * Ends the connection to the Redis server.
   * Does not consider whether the client was created by the record manager or passed by the user.
   */
  async end(): Promise<void> {
    await this.client.quit();
  }

  /**
   * Connects to the Redis server if not already connected.
   */
  private async connectClient(): Promise<void> {
    if (this.client.isOpen) {
      return;
    }
    await this.client.connect();
  }

  /**
   * Creates the index if it does not exist.
   */
  private async createIndex(): Promise<void> {
    if (await this.checkIndex()) {
      return;
    }

    const schema: RediSearchSchema = {
      updatedAt: {
        type: SchemaFieldTypes.NUMERIC,
        TYPE: "FLOAT32",
      },
      groupId: {
        type: SchemaFieldTypes.TEXT,
      },
    };

    await this.client.ft.create(this.indexName, schema, {
      ON: "HASH",
      PREFIX: `${this.namespace}:`,
    });
  }

  /**
   * Checks if the index exists.
   * @returns True if the index exists, false otherwise.
   */
  private async checkIndex(): Promise<boolean> {
    try {
      await this.client.ft.info(this.indexName);
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      if ((err as any).message.includes("unknown command")) {
        throw new Error(
          "Failed to run FT.INFO command. Please ensure that you are running a RediSearch-capable Redis instance: https://js.langchain.com/docs/modules/data_connection/vectorstores/integrations/redis#setup"
        );
      }
      return false;
    }

    return true;
  }

  /**
   * Builds a search query that searches for keys with a given updatedAt timestamp and groupIds.
   * @param {QueryOptions} options
   * @returns A tuple with the first element being the search query and the second element being the search options.
   */
  private buildSearchQuery(options: QueryOptions): [string, SearchOptions] {
    const { before, after, limit: _limit, groupIds, offset } = options;
    const limit = Number.isFinite(_limit) ? _limit : this.searchBatchSize; // default to batch size if limit is not provided
    const searchOptions = { LIMIT: { from: offset, size: limit } };
    const updatedAtSearchQuery = `
      @updatedAt:[(${after ?? 0} (${before ?? "inf"}]
    `;
    const groupIdSearchQuery =
      groupIds && groupIds.length
        ? ` @groupId:(${groupIds.filter((id) => id !== null).join(" | ")})`
        : "";
    const baseQuery = updatedAtSearchQuery + groupIdSearchQuery;
    return [baseQuery, searchOptions];
  }

  private extractKeys(documents: { id: string }[]): string[] {
    return documents.map(({ id }) => id.split(":").slice(-1).join(":"));
  }

  _recordManagerType(): string {
    return "redis";
  }
}
