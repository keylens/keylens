/**
 * A realistic BullMQ workload for keylens development.
 *
 * The point is not "some jobs exist". The point is that every UI surface keylens needs to
 * render has something real behind it:
 *
 *   - failed jobs with genuine multi-frame stack traces (not `new Error('boom')`)
 *   - jobs that retry, so `attemptsMade` and backoff are exercised
 *   - delayed and prioritized jobs, which live in ZSETs with meaningful scores
 *   - a parent/child flow, so `waiting-children` is non-empty
 *   - a queue that pauses and resumes, so `meta.paused` flips while you watch
 *   - continuous throughput, so the events-stream sparkline has a signal to graph
 *
 * Runs until stopped.
 */

import { Queue, Worker, FlowProducer } from 'bullmq';

const connection = {
  host: process.env.REDIS_HOST ?? '127.0.0.1',
  port: Number(process.env.REDIS_PORT ?? 6379),
};

const TICK_MS = Number(process.env.TICK_MS ?? 700);
const JOBS_PER_TICK = Number(process.env.JOBS_PER_TICK ?? 6);

// Keep the finished sets bounded so a long-running fixture doesn't grow without limit,
// while still leaving plenty of history to page through in the UI.
const retention = {
  removeOnComplete: { count: 300 },
  removeOnFail: { count: 500 },
};

/* -------------------------------------------------------------------------- */
/* Failure modes with real stack traces                                        */
/* -------------------------------------------------------------------------- */

class SmtpError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'SmtpError';
    this.code = code;
  }
}

function openSocket(host) {
  throw new SmtpError('ECONNREFUSED', `connect ECONNREFUSED ${host}:587`);
}

function sendViaProvider(payload) {
  return openSocket(payload.host ?? 'smtp.internal.example.com');
}

function deliverEmail(payload) {
  return sendViaProvider(payload);
}

function decodeFrame(buffer) {
  throw new RangeError(
    `offset is out of bounds: requested ${buffer.offset}, buffer length ${buffer.length}`,
  );
}

function resizeImage(payload) {
  return decodeFrame({ offset: payload.offset ?? 4096, length: 1024 });
}

function validatePayload(payload) {
  const missing = ['event', 'signature'].filter((k) => !(k in payload));
  if (missing.length) {
    const err = new TypeError(`webhook payload missing required fields: ${missing.join(', ')}`);
    err.received = Object.keys(payload);
    throw err;
  }
}

async function callUpstream() {
  const err = new Error('upstream timed out after 30000ms');
  err.name = 'TimeoutError';
  err.code = 'ETIMEDOUT';
  throw err;
}

/* -------------------------------------------------------------------------- */
/* Queue definitions                                                           */
/* -------------------------------------------------------------------------- */

const pick = (xs) => xs[Math.floor(Math.random() * xs.length)];
const rand = (n) => Math.floor(Math.random() * n);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const DEFINITIONS = [
  {
    name: 'emails',
    concurrency: 5,
    failureRate: 0.18,
    attempts: 3,
    // Fails deep inside a call chain, so the trace has real frames to render.
    fail: (data) => deliverEmail(data),
    make: () => ({
      to: `user${Math.floor(Math.random() * 9000) + 1000}@example.com`,
      template: pick(['welcome', 'receipt', 'password-reset', 'digest']),
      host: 'smtp.internal.example.com',
    }),
  },
  {
    name: 'image-processing',
    concurrency: 2,
    failureRate: 0.3,
    attempts: 2,
    workMs: 400,
    fail: (data) => resizeImage(data),
    make: () => ({
      assetId: `asset_${Math.random().toString(36).slice(2, 10)}`,
      width: pick([320, 640, 1280, 2560]),
      offset: pick([4096, 8192, 16384]),
    }),
  },
  {
    name: 'webhooks',
    concurrency: 8,
    failureRate: 0.12,
    attempts: 4,
    fail: (data) => validatePayload(data),
    make: () =>
      // Half the payloads are deliberately malformed, so failures are data-driven rather
      // than random -- makes a failed job's payload actually explain its own error.
      Math.random() < 0.5
        ? { event: 'order.created', signature: 'sha256=abc123', orderId: rand(100000) }
        : { orderId: rand(100000) },
  },
  {
    name: 'reports',
    concurrency: 1,
    failureRate: 0.05,
    attempts: 1,
    workMs: 900,
    fail: () => callUpstream(),
    make: () => ({
      kind: pick(['daily-revenue', 'churn', 'cohort']),
      range: pick(['24h', '7d', '30d']),
    }),
  },
  {
    name: 'exports',
    concurrency: 2,
    failureRate: 0.08,
    attempts: 2,
    fail: () => callUpstream(),
    make: () => ({ format: pick(['csv', 'xlsx', 'parquet']), rows: rand(500000) }),
  },
];

/* -------------------------------------------------------------------------- */
/* Wiring                                                                      */
/* -------------------------------------------------------------------------- */

const queues = new Map();
const workers = [];

for (const def of DEFINITIONS) {
  queues.set(def.name, new Queue(def.name, { connection }));

  const worker = new Worker(
    def.name,
    async (job) => {
      if (def.workMs) await sleep(def.workMs * (0.5 + Math.random()));

      // Later attempts succeed more often, so retried jobs visibly recover instead of
      // every failure being terminal.
      const rate = def.failureRate / Math.max(1, job.attemptsMade);
      // `await` matters: some failure modes are async, and an un-awaited rejection here
      // escapes the worker's error handling and takes the whole process down.
      if (Math.random() < rate) await def.fail(job.data);

      await job.updateProgress(100);
      await job.log(`processed ${job.name} in ${def.name}`);
      return { ok: true, at: new Date().toISOString() };
    },
    { connection, concurrency: def.concurrency },
  );

  worker.on('failed', (job, err) => {
    if (job?.attemptsMade === 1) {
      console.log(`[${def.name}] job ${job.id} failed: ${err.name}: ${err.message}`);
    }
  });

  workers.push(worker);
}

const flow = new FlowProducer({ connection });

/* -------------------------------------------------------------------------- */
/* Traffic                                                                     */
/* -------------------------------------------------------------------------- */

async function seed() {
  console.log('seeding backlog...');

  for (const def of DEFINITIONS) {
    const q = queues.get(def.name);

    // A plain backlog.
    await q.addBulk(
      Array.from({ length: 25 }, () => ({
        name: def.name,
        data: def.make(),
        opts: { attempts: def.attempts, backoff: { type: 'exponential', delay: 1000 }, ...retention },
      })),
    );

    // Delayed jobs land in the delayed ZSET with a real future score.
    await q.addBulk(
      Array.from({ length: 8 }, (_, i) => ({
        name: `${def.name}:scheduled`,
        data: def.make(),
        opts: { delay: (i + 1) * 60_000, ...retention },
      })),
    );

    // Prioritized jobs go to the prioritized ZSET, not `wait`.
    await q.addBulk(
      Array.from({ length: 5 }, () => ({
        name: `${def.name}:urgent`,
        data: def.make(),
        opts: { priority: pick([1, 2, 3]), ...retention },
      })),
    );
  }

  // A parent with children populates `waiting-children`, which most tools ignore.
  await flow.add({
    name: 'nightly-rollup',
    queueName: 'reports',
    data: { kind: 'rollup', range: '24h' },
    opts: { ...retention },
    children: [
      { name: 'export-orders', queueName: 'exports', data: { format: 'parquet', rows: 120_000 } },
      { name: 'export-users', queueName: 'exports', data: { format: 'csv', rows: 45_000 } },
      { name: 'render-charts', queueName: 'image-processing', data: { assetId: 'chart_1', width: 1280 } },
    ],
  });

  console.log('seeded.');
}

async function tick() {
  for (let i = 0; i < JOBS_PER_TICK; i++) {
    const def = pick(DEFINITIONS);
    await queues.get(def.name).add(def.name, def.make(), {
      attempts: def.attempts,
      backoff: { type: 'exponential', delay: 1000 },
      ...retention,
    });
  }
}

/**
 * Flip `reports` between paused and running every ~45s.
 *
 * This exists to keep keylens honest: current BullMQ pauses by setting `meta.paused = 1`,
 * it does NOT move `wait` into a `paused` list. A reader that infers paused state from
 * that list will show this queue as running the whole time.
 */
async function pauseCycle() {
  const q = queues.get('reports');
  let paused = false;
  for (;;) {
    await sleep(45_000);
    paused = !paused;
    if (paused) {
      await q.pause();
      console.log('[reports] paused');
    } else {
      await q.resume();
      console.log('[reports] resumed');
    }
  }
}

async function main() {
  console.log(`connecting to redis://${connection.host}:${connection.port}`);
  await seed();
  pauseCycle().catch((e) => console.error('pause cycle stopped:', e));

  console.log(`producing ~${JOBS_PER_TICK} jobs every ${TICK_MS}ms`);
  for (;;) {
    await tick();
    await sleep(TICK_MS);
  }
}

async function shutdown() {
  console.log('\nshutting down...');
  await Promise.allSettled([
    ...workers.map((w) => w.close()),
    ...[...queues.values()].map((q) => q.close()),
    flow.close(),
  ]);
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
