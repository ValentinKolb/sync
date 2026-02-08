import { test, expect, beforeEach } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
import { jobs, ValidationError, DuplicateJobError, type Job } from "../index";

const testSchema = z.object({
  message: z.string(),
});

// Clean up Redis before each test
beforeEach(async () => {
  const keys = await redis.send("KEYS", ["jobs:test:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("send creates a job with correct properties", async () => {
  const q = jobs.create({
    name: "test:send",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "hello" });

  expect(job.id).toBeDefined();
  expect(job.data.message).toBe("hello");
  expect(job.status).toBe("waiting");
  expect(job.attempts).toBe(0);
  expect(job.maxRetries).toBe(0);
  expect(job.timeout).toBe(30000);
  expect(job.createdAt).toBeGreaterThan(0);
});

test("send validates data against schema", async () => {
  const q = jobs.create({
    name: "test:validate",
    schema: testSchema,
    prefix: "jobs:test",
  });

  try {
    // @ts-expect-error - intentionally invalid
    await q.send({ invalid: "data" });
    expect(true).toBe(false); // Should not reach
  } catch (e) {
    expect(e).toBeInstanceOf(ValidationError);
  }
});

test("send with delay creates delayed job", async () => {
  const q = jobs.create({
    name: "test:delay",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "delayed" }, { delay: 5000 });

  expect(job.status).toBe("delayed");
  expect(job.scheduledAt).toBeGreaterThan(Date.now());
});

test("send with at creates scheduled job", async () => {
  const q = jobs.create({
    name: "test:at",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const scheduledTime = Date.now() + 10000;
  const job = await q.send({ message: "scheduled" }, { at: scheduledTime });

  expect(job.status).toBe("delayed");
  expect(job.scheduledAt).toBe(scheduledTime);
});

test("send with interval creates periodic job", async () => {
  const q = jobs.create({
    name: "test:interval",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "periodic" }, { interval: 60000 });

  expect(job.status).toBe("delayed"); // Starts delayed
  expect(job.interval).toBe(60000);
  expect(job.scheduledAt).toBeGreaterThan(Date.now());
});

test("send with interval and startImmediately creates immediate periodic job", async () => {
  const q = jobs.create({
    name: "test:interval-immediate",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "periodic" }, { interval: 60000, startImmediately: true });

  expect(job.status).toBe("waiting"); // Starts immediately
  expect(job.interval).toBe(60000);
});

test("send with retries sets maxRetries", async () => {
  const q = jobs.create({
    name: "test:retries",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "retry" }, { retries: 3 });

  expect(job.maxRetries).toBe(3);
});

test("send with timeout sets timeout", async () => {
  const q = jobs.create({
    name: "test:timeout",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "timeout" }, { timeout: 60000 });

  expect(job.timeout).toBe(60000);
});

test("process executes handler for jobs", async () => {
  const q = jobs.create({
    name: "test:process",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const processed: string[] = [];

  await q.send({ message: "job1" });
  await q.send({ message: "job2" });

  const stop = q.process(
    async (job) => {
      processed.push(job.data.message);
    },
    { pollInterval: 50 },
  );

  // Wait for processing
  await Bun.sleep(200);
  stop();

  expect(processed).toContain("job1");
  expect(processed).toContain("job2");
});

test("process calls onSuccess on successful job", async () => {
  const q = jobs.create({
    name: "test:onsuccess",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let successCalled = false;
  let successJobId = "";

  await q.send({ message: "success" });

  const stop = q.process(
    async () => {
      // Success
    },
    {
      pollInterval: 50,
      onSuccess: (job) => {
        successCalled = true;
        successJobId = job.id;
      },
    },
  );

  await Bun.sleep(200);
  stop();

  expect(successCalled).toBe(true);
  expect(successJobId).toBeDefined();
});

test("process retries failed jobs", async () => {
  const q = jobs.create({
    name: "test:retry",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let attempts = 0;

  await q.send({ message: "retry" }, { retries: 2 });

  const stop = q.process(
    async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error("fail");
      }
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(500);
  stop();

  expect(attempts).toBe(3); // 1 initial + 2 retries
});

test("process calls onError when job permanently fails", async () => {
  const q = jobs.create({
    name: "test:onerror",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let errorCalled = false;
  let errorMessage = "";

  await q.send({ message: "fail" }, { retries: 0 });

  const stop = q.process(
    async () => {
      throw new Error("permanent failure");
    },
    {
      pollInterval: 50,
      onError: (job, error) => {
        errorCalled = true;
        errorMessage = error.message;
      },
    },
  );

  await Bun.sleep(200);
  stop();

  expect(errorCalled).toBe(true);
  expect(errorMessage).toBe("permanent failure");
});

test("process calls onFinally after every attempt", async () => {
  const q = jobs.create({
    name: "test:onfinally",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let finallyCalls = 0;

  await q.send({ message: "finally" }, { retries: 1 });

  const stop = q.process(
    async () => {
      throw new Error("fail");
    },
    {
      pollInterval: 50,
      onFinally: () => {
        finallyCalls++;
      },
    },
  );

  await Bun.sleep(300);
  stop();

  expect(finallyCalls).toBe(2); // 1 initial + 1 retry
});

test("delayed jobs are promoted when ready", async () => {
  const q = jobs.create({
    name: "test:promote",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let processed = false;

  await q.send({ message: "delayed" }, { delay: 100 });

  const stop = q.process(
    async () => {
      processed = true;
    },
    { pollInterval: 50 },
  );

  // Should not be processed yet
  await Bun.sleep(50);
  expect(processed).toBe(false);

  // Should be processed after delay
  await Bun.sleep(200);
  expect(processed).toBe(true);

  stop();
});

test("interval jobs are rescheduled after completion", async () => {
  const q = jobs.create({
    name: "test:interval-reschedule",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let processCount = 0;

  await q.send({ message: "interval" }, { interval: 100, startImmediately: true });

  const stop = q.process(
    async () => {
      processCount++;
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(350);
  stop();

  expect(processCount).toBeGreaterThanOrEqual(2);
});

test("interval jobs are rescheduled after failure", async () => {
  const q = jobs.create({
    name: "test:interval-fail",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let processCount = 0;
  let errorCount = 0;

  await q.send({ message: "interval-fail" }, { interval: 100, startImmediately: true, retries: 0 });

  const stop = q.process(
    async () => {
      processCount++;
      throw new Error("always fail");
    },
    {
      pollInterval: 50,
      onError: () => {
        errorCount++;
      },
    },
  );

  await Bun.sleep(350);
  stop();

  expect(processCount).toBeGreaterThanOrEqual(2);
  expect(errorCount).toBeGreaterThanOrEqual(2);
});

test("concurrent processing with multiple workers", async () => {
  const q = jobs.create({
    name: "test:concurrent",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const processed: string[] = [];
  const startTimes: number[] = [];

  // Send 5 jobs
  for (let i = 0; i < 5; i++) {
    await q.send({ message: `job${i}` });
  }

  const stop = q.process(
    async (job) => {
      startTimes.push(Date.now());
      await Bun.sleep(100); // Simulate work
      processed.push(job.data.message);
    },
    { concurrency: 3, pollInterval: 50 },
  );

  await Bun.sleep(500);
  stop();

  expect(processed.length).toBe(5);

  // Check that some jobs started concurrently (within 50ms of each other)
  const concurrent = startTimes.filter((t, i) => i > 0 && t - startTimes[i - 1] < 50);
  expect(concurrent.length).toBeGreaterThan(0);
});

test("job timeout causes hard fail", async () => {
  const q = jobs.create({
    name: "test:timeout-fail",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "slow" }, { timeout: 100, retries: 0 });

  // Start a processor that will claim the job but not finish it
  let jobClaimed = false;
  const stop = q.process(
    async () => {
      jobClaimed = true;
      // Simulate a hanging job - sleep longer than timeout
      await Bun.sleep(500);
    },
    { pollInterval: 50 },
  );

  // Wait for job to be claimed
  await Bun.sleep(80);
  expect(jobClaimed).toBe(true);

  // Stop processor so we can start a new one that will check timeouts
  stop();

  // Wait for timeout to expire
  await Bun.sleep(100);

  // Start new processor to trigger timeout check
  let timeoutDetected = false;
  const stop2 = q.process(async () => {}, {
    pollInterval: 50,
  });

  // Wait for timeout check to run
  await Bun.sleep(150);
  stop2();

  // Verify job was marked as failed
  const failedCount = await redis.scard("jobs:test:test:timeout-fail:failed");
  expect(failedCount).toBe(1);
});

test("stop function stops processing", async () => {
  const q = jobs.create({
    name: "test:stop",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let processCount = 0;

  const stop = q.process(
    async () => {
      processCount++;
    },
    { pollInterval: 50 },
  );

  stop();

  // Send jobs after stop
  await q.send({ message: "after-stop" });

  await Bun.sleep(200);

  expect(processCount).toBe(0);
});

// ==========================
// list() tests
// ==========================

test("list returns all jobs", async () => {
  const q = jobs.create({
    name: "test:list-all",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "one" });
  await q.send({ message: "two" });
  await q.send({ message: "three" });

  const result = await q.list();

  expect(result.total).toBe(3);
  expect(result.jobs.length).toBe(3);
});

test("list filters by tag", async () => {
  const q = jobs.create({
    name: "test:list-tag",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "agent-a-1" }, { tag: "agent:a" });
  await q.send({ message: "agent-a-2" }, { tag: "agent:a" });
  await q.send({ message: "agent-b-1" }, { tag: "agent:b" });
  await q.send({ message: "no-tag" });

  const resultA = await q.list({ tag: "agent:a" });
  expect(resultA.total).toBe(2);
  expect(resultA.jobs.every((j) => j.tag === "agent:a")).toBe(true);

  const resultB = await q.list({ tag: "agent:b" });
  expect(resultB.total).toBe(1);
  expect(resultB.jobs[0]!.data.message).toBe("agent-b-1");

  const resultAll = await q.list();
  expect(resultAll.total).toBe(4);
});

test("list filters by status", async () => {
  const q = jobs.create({
    name: "test:list-status",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "immediate" });
  await q.send({ message: "delayed" }, { delay: 60000 });
  await q.send({ message: "delayed2" }, { delay: 60000 });

  const waiting = await q.list({ status: "waiting" });
  expect(waiting.total).toBe(1);
  expect(waiting.jobs[0]!.data.message).toBe("immediate");

  const delayed = await q.list({ status: "delayed" });
  expect(delayed.total).toBe(2);
});

test("list filters by tag and status combined", async () => {
  const q = jobs.create({
    name: "test:list-combined",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "a-waiting" }, { tag: "agent:a" });
  await q.send({ message: "a-delayed" }, { tag: "agent:a", delay: 60000 });
  await q.send({ message: "b-waiting" }, { tag: "agent:b" });

  const result = await q.list({ tag: "agent:a", status: "delayed" });
  expect(result.total).toBe(1);
  expect(result.jobs[0]!.data.message).toBe("a-delayed");
});

test("list supports pagination", async () => {
  const q = jobs.create({
    name: "test:list-page",
    schema: testSchema,
    prefix: "jobs:test",
  });

  for (let i = 0; i < 5; i++) {
    await q.send({ message: `msg${i}` });
  }

  const page1 = await q.list({ offset: 0, limit: 2 });
  expect(page1.total).toBe(5);
  expect(page1.jobs.length).toBe(2);

  const page2 = await q.list({ offset: 2, limit: 2 });
  expect(page2.total).toBe(5);
  expect(page2.jobs.length).toBe(2);

  const page3 = await q.list({ offset: 4, limit: 2 });
  expect(page3.total).toBe(5);
  expect(page3.jobs.length).toBe(1);
});

test("list returns empty result for unknown tag", async () => {
  const q = jobs.create({
    name: "test:list-empty",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "hello" });

  const result = await q.list({ tag: "nonexistent" });
  expect(result.total).toBe(0);
  expect(result.jobs.length).toBe(0);
});

test("send with tag stores tag on job", async () => {
  const q = jobs.create({
    name: "test:tag-store",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "tagged" }, { tag: "my-tag" });

  expect(job.tag).toBe("my-tag");
});

// ==========================
// cancel() tests
// ==========================

test("cancel removes a waiting job", async () => {
  const q = jobs.create({
    name: "test:cancel-waiting",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "to-cancel" });

  const cancelled = await q.cancel(job.id);
  expect(cancelled).toBe(true);

  const result = await q.list();
  expect(result.total).toBe(0);
});

test("cancel removes a delayed job", async () => {
  const q = jobs.create({
    name: "test:cancel-delayed",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "to-cancel" }, { delay: 60000 });

  const cancelled = await q.cancel(job.id);
  expect(cancelled).toBe(true);

  const result = await q.list();
  expect(result.total).toBe(0);
});

test("cancel returns false for nonexistent job", async () => {
  const q = jobs.create({
    name: "test:cancel-nonexistent",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const cancelled = await q.cancel("nonexistent-id");
  expect(cancelled).toBe(false);
});

test("cancel removes tag index entry", async () => {
  const q = jobs.create({
    name: "test:cancel-tag",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "tagged" }, { tag: "agent:x" });

  await q.cancel(job.id);

  const result = await q.list({ tag: "agent:x" });
  expect(result.total).toBe(0);
});

test("cancel a completed job", async () => {
  const q = jobs.create({
    name: "test:cancel-completed",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "will-complete" });

  const stop = q.process(async () => {}, { pollInterval: 50 });
  await Bun.sleep(200);
  stop();

  // Job should be completed now
  const completed = await q.list({ status: "completed" });
  expect(completed.total).toBe(1);

  const cancelled = await q.cancel(completed.jobs[0]!.id);
  expect(cancelled).toBe(true);

  const result = await q.list();
  expect(result.total).toBe(0);
});

test("cancel a failed job", async () => {
  const q = jobs.create({
    name: "test:cancel-failed",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "will-fail" }, { retries: 0 });

  const stop = q.process(
    async () => {
      throw new Error("fail");
    },
    { pollInterval: 50 },
  );
  await Bun.sleep(200);
  stop();

  const failed = await q.list({ status: "failed" });
  expect(failed.total).toBe(1);

  const cancelled = await q.cancel(failed.jobs[0]!.id);
  expect(cancelled).toBe(true);

  const result = await q.list();
  expect(result.total).toBe(0);
});

test("cancel a delayed interval job", async () => {
  const q = jobs.create({
    name: "test:cancel-interval",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "periodic" }, { interval: 60000 });
  expect(job.status).toBe("delayed");

  const cancelled = await q.cancel(job.id);
  expect(cancelled).toBe(true);

  const result = await q.list();
  expect(result.total).toBe(0);
});

test("list shows completed and failed jobs after processing", async () => {
  const q = jobs.create({
    name: "test:list-after-process",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "will-succeed" });
  await q.send({ message: "will-fail" }, { retries: 0 });

  let count = 0;
  const stop = q.process(
    async (job) => {
      count++;
      if (job.data.message === "will-fail") {
        throw new Error("fail");
      }
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(300);
  stop();

  const completed = await q.list({ status: "completed" });
  expect(completed.total).toBe(1);
  expect(completed.jobs[0]!.data.message).toBe("will-succeed");

  const failed = await q.list({ status: "failed" });
  expect(failed.total).toBe(1);
  expect(failed.jobs[0]!.data.message).toBe("will-fail");
});

test("tag is preserved after job completes", async () => {
  const q = jobs.create({
    name: "test:tag-after-complete",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "tagged-job" }, { tag: "agent:z" });

  const stop = q.process(async () => {}, { pollInterval: 50 });
  await Bun.sleep(200);
  stop();

  const result = await q.list({ tag: "agent:z" });
  expect(result.total).toBe(1);
  expect(result.jobs[0]!.status).toBe("completed");
  expect(result.jobs[0]!.tag).toBe("agent:z");
});

test("tag is preserved after job fails", async () => {
  const q = jobs.create({
    name: "test:tag-after-fail",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "tagged-fail" }, { tag: "agent:fail", retries: 0 });

  const stop = q.process(
    async () => {
      throw new Error("fail");
    },
    { pollInterval: 50 },
  );
  await Bun.sleep(200);
  stop();

  const result = await q.list({ tag: "agent:fail" });
  expect(result.total).toBe(1);
  expect(result.jobs[0]!.status).toBe("failed");
  expect(result.jobs[0]!.tag).toBe("agent:fail");
});

// ==========================
// Recovery tests
// ==========================

test("recovery re-queues stuck active jobs with retries left", async () => {
  const q = jobs.create({
    name: "test:recovery-retry",
    schema: testSchema,
    prefix: "jobs:test",
  });

  // Manually simulate a stuck active job
  const job: Job<{ message: string }> = {
    id: "stuck-1",
    data: { message: "stuck" },
    status: "active",
    attempts: 1,
    maxRetries: 3,
    timeout: 100,
    createdAt: Date.now() - 1000,
    startedAt: Date.now() - 500, // started 500ms ago, timeout is 100ms
  };

  await redis.hset("jobs:test:test:recovery-retry:data", "stuck-1", JSON.stringify(job));
  await redis.lpush("jobs:test:test:recovery-retry:active", "stuck-1");

  // Starting process() triggers recovery
  let processed = false;
  const stop = q.process(
    async (j) => {
      if (j.id === "stuck-1") processed = true;
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(300);
  stop();

  expect(processed).toBe(true);
});

test("recovery marks stuck active jobs as failed when no retries left", async () => {
  const q = jobs.create({
    name: "test:recovery-fail",
    schema: testSchema,
    prefix: "jobs:test",
  });

  // Simulate stuck job with no retries left
  const job: Job<{ message: string }> = {
    id: "stuck-2",
    data: { message: "stuck" },
    status: "active",
    attempts: 1,
    maxRetries: 0,
    timeout: 100,
    createdAt: Date.now() - 1000,
    startedAt: Date.now() - 500,
  };

  await redis.hset("jobs:test:test:recovery-fail:data", "stuck-2", JSON.stringify(job));
  await redis.lpush("jobs:test:test:recovery-fail:active", "stuck-2");

  const stop = q.process(async () => {}, { pollInterval: 50 });
  await Bun.sleep(200);
  stop();

  const result = await q.list({ status: "failed" });
  expect(result.total).toBe(1);
  expect(result.jobs[0]!.id).toBe("stuck-2");
});

test("recovery reschedules stuck active interval jobs", async () => {
  const q = jobs.create({
    name: "test:recovery-interval",
    schema: testSchema,
    prefix: "jobs:test",
  });

  // Simulate stuck interval job with no retries left
  const job: Job<{ message: string }> = {
    id: "stuck-3",
    data: { message: "interval-stuck" },
    status: "active",
    attempts: 1,
    maxRetries: 0,
    timeout: 100,
    interval: 60000,
    createdAt: Date.now() - 1000,
    startedAt: Date.now() - 500,
  };

  await redis.hset("jobs:test:test:recovery-interval:data", "stuck-3", JSON.stringify(job));
  await redis.lpush("jobs:test:test:recovery-interval:active", "stuck-3");

  const stop = q.process(async () => {}, { pollInterval: 50 });
  await Bun.sleep(200);
  stop();

  // Should be rescheduled as delayed, not failed
  const delayed = await q.list({ status: "delayed" });
  expect(delayed.total).toBe(1);
  expect(delayed.jobs[0]!.id).toBe("stuck-3");

  const failed = await q.list({ status: "failed" });
  expect(failed.total).toBe(0);
});

// ==========================
// Graceful Shutdown tests
// ==========================

test("graceful shutdown waits for active jobs to finish", async () => {
  const q = jobs.create({
    name: "test:graceful",
    schema: testSchema,
    prefix: "jobs:test",
  });

  let jobFinished = false;

  await q.send({ message: "slow-job" });

  const stop = q.process(
    async () => {
      await Bun.sleep(200);
      jobFinished = true;
    },
    { pollInterval: 50 },
  );

  // Wait for job to be claimed
  await Bun.sleep(50);

  // Graceful stop — should wait for active job
  await stop();

  expect(jobFinished).toBe(true);
});

test("graceful shutdown resolves immediately when no active jobs", async () => {
  const q = jobs.create({
    name: "test:graceful-idle",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const stop = q.process(async () => {}, { pollInterval: 50 });

  const start = Date.now();
  await stop();
  const elapsed = Date.now() - start;

  // Should resolve very quickly (no active jobs to wait for)
  expect(elapsed).toBeLessThan(100);
});

// ==========================
// Backoff tests
// ==========================

test("retry with backoff delays retries exponentially", async () => {
  const q = jobs.create({
    name: "test:backoff",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const attemptTimes: number[] = [];

  // backoff: 100ms → retries at ~100ms, ~200ms
  await q.send({ message: "backoff" }, { retries: 2, backoff: 100 });

  const stop = q.process(
    async () => {
      attemptTimes.push(Date.now());
      if (attemptTimes.length < 3) {
        throw new Error("fail");
      }
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(1000);
  await stop();

  expect(attemptTimes.length).toBe(3);

  // First retry should be delayed by ~100ms (backoff * 2^0)
  const delay1 = attemptTimes[1]! - attemptTimes[0]!;
  expect(delay1).toBeGreaterThanOrEqual(80); // some tolerance

  // Second retry should be delayed by ~200ms (backoff * 2^1)
  const delay2 = attemptTimes[2]! - attemptTimes[1]!;
  expect(delay2).toBeGreaterThanOrEqual(160);
});

test("retry without backoff retries immediately", async () => {
  const q = jobs.create({
    name: "test:no-backoff",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const attemptTimes: number[] = [];

  await q.send({ message: "no-backoff" }, { retries: 1 });

  const stop = q.process(
    async () => {
      attemptTimes.push(Date.now());
      if (attemptTimes.length < 2) {
        throw new Error("fail");
      }
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(300);
  await stop();

  expect(attemptTimes.length).toBe(2);

  // Should retry very quickly (within one poll cycle)
  const delay = attemptTimes[1]! - attemptTimes[0]!;
  expect(delay).toBeLessThan(150);
});

test("invalid job payload is marked failed", async () => {
  const q = jobs.create({
    name: "test:invalid-payload",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const jobId = "invalid-1";
  await redis.hset("jobs:test:test:invalid-payload:data", jobId, "not-json");
  await redis.lpush("jobs:test:test:invalid-payload:waiting", jobId);

  const stop = q.process(async () => {}, { pollInterval: 50 });
  await Bun.sleep(200);
  await stop();

  const failedCount = await redis.scard("jobs:test:test:invalid-payload:failed");
  expect(failedCount).toBe(1);

  const stored = await redis.hget("jobs:test:test:invalid-payload:data", jobId);
  expect(stored).not.toBeNull();

  const job = JSON.parse(stored!) as Job<{ message: string }>;
  expect(job.status).toBe("failed");
  expect(job.invalidData).toBe(true);
});

// ==========================
// Deduplication tests
// ==========================

test("duplicate key throws DuplicateJobError", async () => {
  const q = jobs.create({
    name: "test:dedup",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "first" }, { key: "unique-1" });

  try {
    await q.send({ message: "duplicate" }, { key: "unique-1" });
    expect(true).toBe(false); // Should not reach
  } catch (e) {
    expect(e).toBeInstanceOf(DuplicateJobError);
    expect((e as DuplicateJobError).key).toBe("unique-1");
  }
});

test("different keys allow separate jobs", async () => {
  const q = jobs.create({
    name: "test:dedup-diff",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job1 = await q.send({ message: "a" }, { key: "key-a" });
  const job2 = await q.send({ message: "b" }, { key: "key-b" });

  expect(job1.id).not.toBe(job2.id);
});

test("jobs without key are never deduplicated", async () => {
  const q = jobs.create({
    name: "test:dedup-nokey",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job1 = await q.send({ message: "same" });
  const job2 = await q.send({ message: "same" });

  expect(job1.id).not.toBe(job2.id);
  const result = await q.list();
  expect(result.total).toBe(2);
});

test("unique key is released after job completes", async () => {
  const q = jobs.create({
    name: "test:dedup-release",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "first" }, { key: "release-key" });

  const stop = q.process(async () => {}, { pollInterval: 50 });
  await Bun.sleep(200);
  await stop();

  // Key should be released, can send again
  const job2 = await q.send({ message: "second" }, { key: "release-key" });
  expect(job2.id).toBeDefined();
});

test("unique key is released after job permanently fails", async () => {
  const q = jobs.create({
    name: "test:dedup-fail-release",
    schema: testSchema,
    prefix: "jobs:test",
  });

  await q.send({ message: "fail" }, { key: "fail-key", retries: 0 });

  const stop = q.process(
    async () => {
      throw new Error("fail");
    },
    { pollInterval: 50 },
  );
  await Bun.sleep(200);
  await stop();

  // Key should be released
  const job2 = await q.send({ message: "retry" }, { key: "fail-key" });
  expect(job2.id).toBeDefined();
});

test("unique key is released after cancel", async () => {
  const q = jobs.create({
    name: "test:dedup-cancel",
    schema: testSchema,
    prefix: "jobs:test",
  });

  const job = await q.send({ message: "cancel-me" }, { key: "cancel-key" });

  await q.cancel(job.id);

  // Key should be released
  const job2 = await q.send({ message: "new" }, { key: "cancel-key" });
  expect(job2.id).toBeDefined();
});

test("unique key persists during retries", async () => {
  const q = jobs.create({
    name: "test:dedup-retry-persist",
    schema: testSchema,
    prefix: "jobs:test",
  });

  // Use backoff to keep the job in delayed state between retries
  await q.send({ message: "retry-dedup" }, { key: "retry-key", retries: 3, backoff: 500 });

  const stop = q.process(
    async () => {
      throw new Error("fail");
    },
    { pollInterval: 50 },
  );

  // Wait for first attempt to fail — job should be in delayed with backoff
  await Bun.sleep(150);

  try {
    await q.send({ message: "dup" }, { key: "retry-key" });
    expect(true).toBe(false);
  } catch (e) {
    expect(e).toBeInstanceOf(DuplicateJobError);
  }

  await stop();
});
