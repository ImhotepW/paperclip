import { AsyncLocalStorage } from "node:async_hooks";
import { logger } from "../../middleware/logger.js";

// Instance-wide cap on concurrently running heartbeat runs, for deployments
// where every agent shares one constrained resource (for example a single
// local inference host). Set PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS to a
// positive integer to enable it; unset, empty, zero, or invalid values leave
// admission limited only by each agent's own maxConcurrentRuns.
//
// Admission is coordinated inside this server process: the running count is
// read from the database under a process-wide lock, so the cap holds for one
// Paperclip server process per database. It does not coordinate several
// server processes sharing a database.
export const GLOBAL_MAX_CONCURRENT_RUNS_ENV = "PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS";

const GLOBAL_ADMISSION_LOCK_STALE_MS = 30_000;

export function readGlobalMaxConcurrentRuns(env: NodeJS.ProcessEnv = process.env): number | null {
  const raw = env[GLOBAL_MAX_CONCURRENT_RUNS_ENV]?.trim();
  if (!raw || !/^\d+$/.test(raw)) return null;
  const limit = Number(raw);
  return Number.isSafeInteger(limit) && limit > 0 ? limit : null;
}

// Claims can promote or cancel other runs while the lock is held, and those
// paths re-enter queue admission. The holder's async context marks nested
// admission as already inside the critical section instead of deadlocking on it.
const admissionContext = new AsyncLocalStorage<true>();
let lockTail: { promise: Promise<void>; startedAtMs: number } | null = null;

async function waitForPreviousHolder(previous: { promise: Promise<void>; startedAtMs: number }) {
  const remainingMs = GLOBAL_ADMISSION_LOCK_STALE_MS - (Date.now() - previous.startedAtMs);
  if (remainingMs <= 0) {
    logger.warn({ staleMs: Date.now() - previous.startedAtMs }, "global admission lock stale; continuing queued-run admission");
    return;
  }
  let timeout: ReturnType<typeof setTimeout> | null = null;
  const timedOut = await Promise.race([
    previous.promise.then(() => false),
    new Promise<boolean>((resolve) => {
      timeout = setTimeout(() => resolve(true), remainingMs);
    }),
  ]);
  if (timeout) clearTimeout(timeout);
  if (timedOut) {
    logger.warn({ staleMs: GLOBAL_ADMISSION_LOCK_STALE_MS }, "global admission lock timed out; continuing queued-run admission");
  }
}

export async function withGlobalAdmissionLock<T>(fn: () => Promise<T>): Promise<T> {
  if (admissionContext.getStore()) return fn();
  const previous = lockTail;
  const run = (previous ? waitForPreviousHolder(previous) : Promise.resolve()).then(() => admissionContext.run(true, fn));
  const marker = run.then(
    () => undefined,
    () => undefined,
  );
  const entry = { promise: marker, startedAtMs: Date.now() };
  lockTail = entry;
  try {
    return await run;
  } finally {
    if (lockTail === entry) lockTail = null;
  }
}
