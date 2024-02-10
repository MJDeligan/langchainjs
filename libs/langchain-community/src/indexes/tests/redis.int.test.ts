import { describe, expect, test, jest } from "@jest/globals";
import { RedisRecordManager } from "../redis.js";

describe.skip("RedisRecordManager", () => {
  const namespace = "test";
  const indexName = "myIndex";
  let recordManager: RedisRecordManager;

  beforeAll(async () => {
    const config = {
      url: "redis://127.0.0.1:6379",
    };
    recordManager = new RedisRecordManager(namespace, {
      indexName,
      redisClientOptions: config,
    });
    await recordManager.createSchema();
  });

  afterEach(async () => {
    // Drop index, then recreate it for the next test.
    await recordManager.client.ft.dropIndex(indexName, { DD: true });
    await recordManager.createSchema();
  });

  afterAll(async () => {
    await recordManager.client.ft.dropIndex(indexName, { DD: true });
    await recordManager.end();
  });

  test("Test upsertion", async () => {
    const keys = ["a", "b", "c"];
    await recordManager.update(keys);
    const readKeys = await recordManager.listKeys();
    expect(readKeys).toEqual(expect.arrayContaining(keys));
    expect(readKeys).toHaveLength(keys.length);
  });

  test("Test upsertion with timeAtLeast", async () => {
    // Mock getTime to return 100.
    const unmockedGetTime = recordManager.getTime;
    recordManager.getTime = jest.fn(() => Promise.resolve(100));

    const keys = ["a", "b", "c"];
    await expect(
      recordManager.update(keys, { timeAtLeast: 110 })
    ).rejects.toThrowError();
    const readKeys = await recordManager.listKeys();
    expect(readKeys).toHaveLength(0);

    // Set getTime back to normal.
    recordManager.getTime = unmockedGetTime;
  });

  test("Test update timestamp", async () => {
    const unmockedGetTime = recordManager.getTime;
    recordManager.getTime = jest.fn(() => Promise.resolve(100));
    try {
      const keys = ["a", "b", "c"];
      await recordManager.update(keys);
      const res = await recordManager.client.ft.search(indexName, "*");
      res.documents.forEach(
        ({ value }) => expect(value.updatedAt).toEqual("100") // Value returned as string
      );

      recordManager.getTime = jest.fn(() => Promise.resolve(200));
      await recordManager.update(keys);
      const res2 = await recordManager.client.ft.search(indexName, "*");
      res2.documents.forEach(({ value }) =>
        expect(value.updatedAt).toEqual("200")
      );
    } finally {
      recordManager.getTime = unmockedGetTime;
    }
  });

  test("Test update with groupIds", async () => {
    const keys = ["a", "b", "c"];
    await recordManager.update(keys, {
      groupIds: ["group1", "group1", "group2"],
    });
    const res = await recordManager.client.ft.search(
      indexName,
      "@groupId:group1"
    );
    expect(res.total).toEqual(2);
    res.documents.forEach(({ value }) =>
      expect(value.groupId).toEqual("group1")
    );
  });

  test("Exists", async () => {
    const keys = ["a", "b", "c"];
    await recordManager.update(keys);
    const exists = await recordManager.exists(keys);
    expect(exists).toEqual([true, true, true]);

    const nonExistentKeys = ["d", "e", "f"];
    const nonExists = await recordManager.exists(nonExistentKeys);
    expect(nonExists).toEqual([false, false, false]);

    const mixedKeys = ["a", "e", "c"];
    const mixedExists = await recordManager.exists(mixedKeys);
    expect(mixedExists).toEqual([true, false, true]);
  });

  test("Delete", async () => {
    const keys = ["a", "b", "c"];
    await recordManager.update(keys);
    await recordManager.deleteKeys(["a", "c"]);
    const readKeys = await recordManager.listKeys();
    expect(readKeys).toEqual(["b"]);
  });

  test("List keys", async () => {
    const unmockedGetTime = recordManager.getTime;
    recordManager.getTime = jest.fn(() => Promise.resolve(100));
    try {
      const keys = ["a", "b", "c"];
      await recordManager.update(keys);
      const readKeys = await recordManager.listKeys();
      expect(readKeys).toEqual(expect.arrayContaining(keys));
      expect(readKeys).toHaveLength(keys.length);

      // All keys inserted after 90: should be all keys
      const readKeysAfterInsertedAfter = await recordManager.listKeys({
        after: 90,
      });
      expect(readKeysAfterInsertedAfter).toEqual(expect.arrayContaining(keys));

      // All keys inserted after 110: should be none
      const readKeysAfterInsertedBefore = await recordManager.listKeys({
        after: 110,
      });
      expect(readKeysAfterInsertedBefore).toEqual([]);

      // All keys inserted before 110: should be all keys
      const readKeysBeforeInsertedBefore = await recordManager.listKeys({
        before: 110,
      });
      expect(readKeysBeforeInsertedBefore).toEqual(
        expect.arrayContaining(keys)
      );

      // All keys inserted before 90: should be none
      const readKeysBeforeInsertedAfter = await recordManager.listKeys({
        before: 90,
      });
      expect(readKeysBeforeInsertedAfter).toEqual([]);

      // Set one key to updated at 120 and one at 80
      recordManager.getTime = jest.fn(() => Promise.resolve(120));
      await recordManager.update(["a"]);
      recordManager.getTime = jest.fn(() => Promise.resolve(80));
      await recordManager.update(["b"]);

      // All keys updated after 90 and before 110: should only be "c" now
      const readKeysBeforeAndAfter = await recordManager.listKeys({
        before: 110,
        after: 90,
      });
      expect(readKeysBeforeAndAfter).toEqual(["c"]);
    } finally {
      recordManager.getTime = unmockedGetTime;
    }
  });

  test("List keys with groupIds", async () => {
    const keys = ["a", "b", "c"];
    await recordManager.update(keys, {
      groupIds: ["group1", "group1", "group2"],
    });
    const readKeys = await recordManager.listKeys({ groupIds: ["group1"] });
    expect(readKeys).toEqual(["a", "b"]);
  });

  test("Handles more than 100 keys", async () => {
    // The default redis search result limit is 10, so this test ensures that
    // the listKeys function can handle pagination
    const keys = Array.from({ length: 101 }, (_, i) => i.toString());
    await recordManager.update(keys);
    const readKeys = await recordManager.listKeys();
    expect(readKeys).toEqual(expect.arrayContaining(keys));
  });
});
