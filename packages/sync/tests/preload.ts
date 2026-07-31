import { redis } from "bun";

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  throw new Error("server tests require REDIS_URL to be set before Bun starts");
}

const parsedRedisUrl = new URL(redisUrl);
if (
  (parsedRedisUrl.hostname !== "127.0.0.1" && parsedRedisUrl.hostname !== "localhost")
  || parsedRedisUrl.port !== "6399"
) {
  throw new Error("server tests require an isolated Redis at localhost:6399");
}

if (await redis.get("sync:test:sentinel") !== "k2b-sync-test-v1") {
  throw new Error("server tests require the dedicated Sync test Redis sentinel");
}
