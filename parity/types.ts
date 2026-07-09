/**
 * Public API type parity — server vs browser.
 *
 * This file does NOT run at runtime. It is type-checked by tsc via
 * `parity/tsconfig.json`. If any shared public type diverges between
 * the two packages, compile fails here.
 *
 * Add new shared types to the assertions below when the API grows.
 */

// ==========================
// Server imports
// ==========================

import type {
  // ratelimit
  RateLimiter as S_RateLimiter,
  RateLimitResult as S_RateLimitResult,
  RateLimitConfig as S_RateLimitConfig,
} from "../packages/sync/src/ratelimit";
import type {
  Mutex as S_Mutex,
  Lock as S_Lock,
  MutexConfig as S_MutexConfig,
} from "../packages/sync/src/mutex";
import type {
  Queue as S_Queue,
  QueueConfig as S_QueueConfig,
  QueueReader as S_QueueReader,
  QueueRecvConfig as S_QueueRecvConfig,
  QueueSendConfig as S_QueueSendConfig,
  QueueReceived as S_QueueReceived,
} from "../packages/sync/src/queue";
import type {
  Topic as S_Topic,
  TopicConfig as S_TopicConfig,
  TopicCursorConfig as S_TopicCursorConfig,
  TopicReader as S_TopicReader,
  TopicRecvConfig as S_TopicRecvConfig,
  TopicPubConfig as S_TopicPubConfig,
  TopicDelivery as S_TopicDelivery,
  TopicLiveConfig as S_TopicLiveConfig,
  TopicLiveEvent as S_TopicLiveEvent,
} from "../packages/sync/src/topic";
import type {
  EphemeralConfig as S_EphemeralConfig,
  EphemeralUpsertConfig as S_EphemeralUpsertConfig,
  EphemeralTouchConfig as S_EphemeralTouchConfig,
  EphemeralRemoveConfig as S_EphemeralRemoveConfig,
  EphemeralEntry as S_EphemeralEntry,
  EphemeralSnapshot as S_EphemeralSnapshot,
  EphemeralRecvConfig as S_EphemeralRecvConfig,
  EphemeralEvent as S_EphemeralEvent,
  EphemeralReader as S_EphemeralReader,
  EphemeralStore as S_EphemeralStore,
} from "../packages/sync/src/ephemeral";
import type {
  JobId as S_JobId,
  JobCtx as S_JobCtx,
  JobAfterCtx as S_JobAfterCtx,
  JobConfig as S_JobConfig,
  JobHandle as S_JobHandle,
  JobMetrics as S_JobMetrics,
  JobTraceEvent as S_JobTraceEvent,
  SubmitConfig as S_SubmitConfig,
} from "../packages/sync/src/job";
import type {
  Scheduler as S_Scheduler,
  SchedulerConfig as S_SchedulerConfig,
  SchedulerInfo as S_SchedulerInfo,
  SchedulerMetrics as S_SchedulerMetrics,
  SchedulerTraceEvent as S_SchedulerTraceEvent,
  ScheduleConfig as S_ScheduleConfig,
  ScheduleCtx as S_ScheduleCtx,
  ScheduleAfterCtx as S_ScheduleAfterCtx,
} from "../packages/sync/src/scheduler";
import type {
  SchedulerControl as S_SchedulerControl,
  SchedulerControlConfig as S_SchedulerControlConfig,
  SchedulerControlInfo as S_SchedulerControlInfo,
  SchedulerControlRunNowConfig as S_SchedulerControlRunNowConfig,
  SchedulerControlState as S_SchedulerControlState,
} from "../packages/sync/src/scheduler-control";
import type {
  TraceHandler as S_TraceHandler,
} from "../packages/sync/src/trace";
import type {
  BackoffOptions as S_BackoffOptions,
  RetryCtx as S_RetryCtx,
  RetryAfterCtx as S_RetryAfterCtx,
  RetryConfig as S_RetryConfig,
} from "../packages/sync/src/retry";

// ==========================
// Browser imports
// ==========================

import type {
  RateLimiter as B_RateLimiter,
  RateLimitResult as B_RateLimitResult,
  RateLimitConfig as B_RateLimitConfig,
} from "../packages/sync-browser/src/ratelimit";
import type {
  Mutex as B_Mutex,
  Lock as B_Lock,
  MutexConfig as B_MutexConfig,
} from "../packages/sync-browser/src/mutex";
import type {
  Queue as B_Queue,
  QueueConfig as B_QueueConfig,
  QueueReader as B_QueueReader,
  QueueRecvConfig as B_QueueRecvConfig,
  QueueSendConfig as B_QueueSendConfig,
  QueueReceived as B_QueueReceived,
} from "../packages/sync-browser/src/queue";
import type {
  Topic as B_Topic,
  TopicConfig as B_TopicConfig,
  TopicCursorConfig as B_TopicCursorConfig,
  TopicReader as B_TopicReader,
  TopicRecvConfig as B_TopicRecvConfig,
  TopicPubConfig as B_TopicPubConfig,
  TopicDelivery as B_TopicDelivery,
  TopicLiveConfig as B_TopicLiveConfig,
  TopicLiveEvent as B_TopicLiveEvent,
} from "../packages/sync-browser/src/topic";
import type {
  EphemeralConfig as B_EphemeralConfig,
  EphemeralUpsertConfig as B_EphemeralUpsertConfig,
  EphemeralTouchConfig as B_EphemeralTouchConfig,
  EphemeralRemoveConfig as B_EphemeralRemoveConfig,
  EphemeralEntry as B_EphemeralEntry,
  EphemeralSnapshot as B_EphemeralSnapshot,
  EphemeralRecvConfig as B_EphemeralRecvConfig,
  EphemeralEvent as B_EphemeralEvent,
  EphemeralReader as B_EphemeralReader,
  EphemeralStore as B_EphemeralStore,
} from "../packages/sync-browser/src/ephemeral";
import type {
  JobId as B_JobId,
  JobCtx as B_JobCtx,
  JobAfterCtx as B_JobAfterCtx,
  JobConfig as B_JobConfig,
  JobHandle as B_JobHandle,
  JobMetrics as B_JobMetrics,
  JobTraceEvent as B_JobTraceEvent,
  SubmitConfig as B_SubmitConfig,
} from "../packages/sync-browser/src/job";
import type {
  Scheduler as B_Scheduler,
  SchedulerConfig as B_SchedulerConfig,
  SchedulerInfo as B_SchedulerInfo,
  SchedulerMetrics as B_SchedulerMetrics,
  SchedulerTraceEvent as B_SchedulerTraceEvent,
  ScheduleConfig as B_ScheduleConfig,
  ScheduleCtx as B_ScheduleCtx,
  ScheduleAfterCtx as B_ScheduleAfterCtx,
} from "../packages/sync-browser/src/scheduler";
import type {
  SchedulerControl as B_SchedulerControl,
  SchedulerControlConfig as B_SchedulerControlConfig,
  SchedulerControlInfo as B_SchedulerControlInfo,
  SchedulerControlRunNowConfig as B_SchedulerControlRunNowConfig,
  SchedulerControlState as B_SchedulerControlState,
} from "../packages/sync-browser/src/scheduler-control";
import type {
  TraceHandler as B_TraceHandler,
} from "../packages/sync-browser/src/trace";
import type {
  BackoffOptions as B_BackoffOptions,
  RetryCtx as B_RetryCtx,
  RetryAfterCtx as B_RetryAfterCtx,
  RetryConfig as B_RetryConfig,
} from "../packages/sync-browser/src/retry";

// ==========================
// Structural equality helper
// ==========================

/**
 * Bidirectional subtype assertion. Yields `true` only if A and B are
 * mutually assignable (structurally equivalent). Otherwise `never`,
 * which fails type-check when assigned to a `true` literal.
 *
 * Note: This is stricter than subtype in both directions, so optional
 * properties and function parameter variance are caught.
 */
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2)
    ? true
    : never;

const assertEqual = <T extends true>(_: T): void => {
  /* compile-time only */
};

// ==========================
// Ratelimit
// ==========================

assertEqual<Equal<S_RateLimiter, B_RateLimiter>>(true);
assertEqual<Equal<S_RateLimitResult, B_RateLimitResult>>(true);
// RateLimitConfig differs: browser has optional `store?: Store` additive
const _rlCfgAdditive: B_RateLimitConfig = {} as unknown as S_RateLimitConfig;
void _rlCfgAdditive;

// ==========================
// Mutex
// ==========================

assertEqual<Equal<S_Mutex, B_Mutex>>(true);
assertEqual<Equal<S_Lock, B_Lock>>(true);
// MutexConfig differs: browser has additional `store?: Store` additive field.
// Assert instead that S_MutexConfig is assignable to B_MutexConfig
// (server config is valid on browser).
const _mutexCfgAdditive: B_MutexConfig = {} as unknown as S_MutexConfig;
void _mutexCfgAdditive;

// ==========================
// Queue
// ==========================

assertEqual<Equal<S_Queue<{ foo: string }>, B_Queue<{ foo: string }>>>(true);
assertEqual<Equal<S_QueueConfig<{ foo: string }>, B_QueueConfig<{ foo: string }>>>(true);
assertEqual<Equal<S_QueueReader<{ foo: string }>, B_QueueReader<{ foo: string }>>>(true);
assertEqual<Equal<S_QueueRecvConfig, B_QueueRecvConfig>>(true);
assertEqual<Equal<S_QueueSendConfig<{ foo: string }>, B_QueueSendConfig<{ foo: string }>>>(true);
assertEqual<Equal<S_QueueReceived<{ foo: string }>, B_QueueReceived<{ foo: string }>>>(true);

// ==========================
// Topic
// ==========================

assertEqual<Equal<S_Topic<{ foo: string }>, B_Topic<{ foo: string }>>>(true);
// TopicConfig differs: browser has optional `store?` additive field
const _topicCfgAdditive: B_TopicConfig<{ foo: string }> = {} as unknown as S_TopicConfig<{ foo: string }>;
void _topicCfgAdditive;
assertEqual<Equal<S_TopicReader<{ foo: string }>, B_TopicReader<{ foo: string }>>>(true);
assertEqual<Equal<S_TopicCursorConfig, B_TopicCursorConfig>>(true);
assertEqual<Equal<S_TopicRecvConfig, B_TopicRecvConfig>>(true);
assertEqual<Equal<S_TopicPubConfig<{ foo: string }>, B_TopicPubConfig<{ foo: string }>>>(true);
assertEqual<Equal<S_TopicDelivery<{ foo: string }>, B_TopicDelivery<{ foo: string }>>>(true);
assertEqual<Equal<S_TopicLiveConfig, B_TopicLiveConfig>>(true);
assertEqual<Equal<S_TopicLiveEvent<{ foo: string }>, B_TopicLiveEvent<{ foo: string }>>>(true);

// ==========================
// Ephemeral
// ==========================

assertEqual<Equal<S_EphemeralConfig<{ v: number }>, B_EphemeralConfig<{ v: number }>>>(true);
assertEqual<Equal<S_EphemeralUpsertConfig<{ v: number }>, B_EphemeralUpsertConfig<{ v: number }>>>(true);
assertEqual<Equal<S_EphemeralTouchConfig, B_EphemeralTouchConfig>>(true);
assertEqual<Equal<S_EphemeralRemoveConfig, B_EphemeralRemoveConfig>>(true);
assertEqual<Equal<S_EphemeralEntry<{ v: number }>, B_EphemeralEntry<{ v: number }>>>(true);
assertEqual<Equal<S_EphemeralSnapshot<{ v: number }>, B_EphemeralSnapshot<{ v: number }>>>(true);
assertEqual<Equal<S_EphemeralRecvConfig, B_EphemeralRecvConfig>>(true);
assertEqual<Equal<S_EphemeralEvent<{ v: number }>, B_EphemeralEvent<{ v: number }>>>(true);
assertEqual<Equal<S_EphemeralReader<{ v: number }>, B_EphemeralReader<{ v: number }>>>(true);
assertEqual<Equal<S_EphemeralStore<{ v: number }>, B_EphemeralStore<{ v: number }>>>(true);

// ==========================
// Job
// ==========================

assertEqual<Equal<S_JobId, B_JobId>>(true);
assertEqual<Equal<S_JobCtx<{ userId: string }>, B_JobCtx<{ userId: string }>>>(true);
assertEqual<Equal<S_JobCtx<void>, B_JobCtx<void>>>(true);
assertEqual<Equal<S_JobAfterCtx<{ userId: string }, number>, B_JobAfterCtx<{ userId: string }, number>>>(true);
assertEqual<Equal<S_JobAfterCtx<void, void>, B_JobAfterCtx<void, void>>>(true);
assertEqual<Equal<S_JobConfig<{ userId: string }, number>, B_JobConfig<{ userId: string }, number>>>(true);
assertEqual<Equal<S_JobHandle<{ userId: string }>, B_JobHandle<{ userId: string }>>>(true);
assertEqual<Equal<S_JobHandle<void>, B_JobHandle<void>>>(true);
assertEqual<Equal<S_JobMetrics, B_JobMetrics>>(true);
assertEqual<Equal<S_JobTraceEvent<{ userId: string }, number>, B_JobTraceEvent<{ userId: string }, number>>>(true);
assertEqual<Equal<S_SubmitConfig<{ userId: string }>, B_SubmitConfig<{ userId: string }>>>(true);
assertEqual<Equal<S_SubmitConfig<void>, B_SubmitConfig<void>>>(true);

// ==========================
// Scheduler
// ==========================

assertEqual<Equal<S_Scheduler, B_Scheduler>>(true);
// SchedulerConfig differs: browser has optional `store?` additive
const _schedCfgAdditive: B_SchedulerConfig = {} as unknown as S_SchedulerConfig;
void _schedCfgAdditive;
assertEqual<Equal<S_SchedulerInfo, B_SchedulerInfo>>(true);
assertEqual<Equal<S_SchedulerMetrics, B_SchedulerMetrics>>(true);
assertEqual<Equal<S_SchedulerTraceEvent<number>, B_SchedulerTraceEvent<number>>>(true);
assertEqual<Equal<S_ScheduleConfig<number>, B_ScheduleConfig<number>>>(true);
assertEqual<Equal<S_ScheduleCtx, B_ScheduleCtx>>(true);
assertEqual<Equal<S_ScheduleAfterCtx<number>, B_ScheduleAfterCtx<number>>>(true);
assertEqual<Equal<S_SchedulerControl, B_SchedulerControl>>(true);
assertEqual<Equal<S_SchedulerControlConfig, B_SchedulerControlConfig>>(true);
assertEqual<Equal<S_SchedulerControlInfo, B_SchedulerControlInfo>>(true);
assertEqual<Equal<S_SchedulerControlRunNowConfig, B_SchedulerControlRunNowConfig>>(true);
assertEqual<Equal<S_SchedulerControlState, B_SchedulerControlState>>(true);

// ==========================
// Trace
// ==========================

assertEqual<Equal<S_TraceHandler<{ type: "x" }>, B_TraceHandler<{ type: "x" }>>>(true);

// ==========================
// Retry
// ==========================

assertEqual<Equal<S_BackoffOptions, B_BackoffOptions>>(true);
assertEqual<Equal<S_RetryCtx, B_RetryCtx>>(true);
assertEqual<Equal<S_RetryAfterCtx<number>, B_RetryAfterCtx<number>>>(true);
assertEqual<Equal<S_RetryConfig<number>, B_RetryConfig<number>>>(true);
