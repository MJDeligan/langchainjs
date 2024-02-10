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
  /**
   * Whether to log a warning message about redis' maximum search results limit. Defaults to true.
   */
  verbose?: boolean;
}

type QueryOptions = ListKeyOptions;

export class RedisRecordManager extends RecordManager {
  maxSearchResults?: number;

  verbose: boolean;

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
    const {
      indexName,
      redisClientOptions,
      redisClient,
      verbose = true,
    } = config;
    if (!redisClientOptions && !redisClient) {
      throw new Error(
        "Either redisClientOptions or redisClient must be provided."
      );
    }
    this.namespace = namespace;
    this.indexName = indexName ?? this.indexName;
    this.verbose = verbose;
    this.client = redisClient ?? createClient(redisClientOptions);
  }

  /**
   * Sets up the record manager by connecting to the Redis server and creating the index.
   */
  async createSchema(): Promise<void> {
    await this.connectClient();
    await this.setMaxSearchResults();
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
    const { limit } = options ?? {};

    // Redis search has a limit on the amount of search results it can return.
    // We cannot use pagination because the offset cannot exceed the maximum search results.
    try {
      const result = await this.client.ft.search(
        this.indexName,
        ...this.buildSearchQuery({
          ...options,
          limit,
        })
      );
      // if no limit is provided, we throw an error if not all keys could be retrieved
      if (limit === undefined && result.total > result.documents.length) {
        throw new Error(
          `Could not retrieve all keys. The amount of keys exceeds the maximum search results of ${this.maxSearchResults}.
          See https://redis.io/docs/interact/search-and-query/basic-constructs/configuration-parameters/#maxsearchresults for configuration options`
        );
      }

      return this.extractKeys(result.documents);
    } catch (err) {
      // If the error is due to the limit exceeding the maximum search results, throw a more informative error.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      if ((err as any).message.includes("LIMIT exceeds maximum")) {
        throw new Error(
          `The limit ${
            limit ?? this.maxSearchResults
          } exceeds the maximum search results of ${this.maxSearchResults}.
          See https://redis.io/docs/interact/search-and-query/basic-constructs/configuration-parameters/#maxsearchresults for configuration options`
        );
      }
      throw err;
    }
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
   * Sets the maximum number of search results we retrieve in a single query.
   */
  private async setMaxSearchResults() {
    const configValue = Number.parseInt(
      (await this.client.ft.CONFIG_GET("MAXSEARCHRESULTS"))
        .MAXSEARCHRESULTS as string,
      10
    );

    this.maxSearchResults = configValue;

    if (this.verbose) {
      console.warn(
        `
        The maximum number of search results is set to ${configValue}.
        If you expect to have more than this amount of keys, consider increasing the value of MAXSEARCHRESULTS in the redis configuration.
        See https://redis.io/docs/interact/search-and-query/basic-constructs/configuration-parameters/#maxsearchresults for configuration options`
      );
    }
  }

  /**
   * Builds a search query that searches for keys with a given updatedAt timestamp and groupIds.
   * @param {QueryOptions} options
   * @returns A tuple with the first element being the search query and the second element being the search options.
   */
  private buildSearchQuery(options: QueryOptions): [string, SearchOptions] {
    const {
      before,
      after,
      limit = this.maxSearchResults as number,
      groupIds,
    } = options;

    const searchOptions = { LIMIT: { from: 0, size: limit } };
    const updatedAtSearchQuery = `
      @updatedAt:[(${after ?? 0} (${before ?? "inf"}]
    `;
    const groupIdSearchQuery =
      groupIds && groupIds.length
        ? ` @groupId:(${groupIds.filter((id) => id !== null).join(" | ")})`
        : "";
    const baseQuery = this.escapeQuery(
      updatedAtSearchQuery + groupIdSearchQuery
    );
    return [baseQuery, searchOptions];
  }

  private extractKeys(documents: { id: string }[]): string[] {
    return documents.map(({ id }) => id.split(":").slice(-1).join(":"));
  }

  private escapeQuery(query: string): string {
    return query.replace("-", "\\-");
  }

  _recordManagerType(): string {
    return "redis";
  }
}
