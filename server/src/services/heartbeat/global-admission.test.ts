import { describe, expect, it } from "vitest";
import { readGlobalMaxConcurrentRuns, withGlobalAdmissionLock } from "./global-admission.js";

describe("global admission cap", () => {
  it.each([
    [undefined, null],
    ["", null],
    ["  ", null],
    ["0", null],
    ["-1", null],
    ["1.5", null],
    ["two", null],
    ["1", 1],
    [" 3 ", 3],
  ])("reads %j as %j", (raw, expected) => {
    expect(readGlobalMaxConcurrentRuns({ PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS: raw })).toBe(expected);
  });

  it("serializes holders in arrival order", async () => {
    const events: string[] = [];
    let releaseFirst!: () => void;
    const first = withGlobalAdmissionLock(async () => {
      events.push("first:start");
      await new Promise<void>((resolve) => { releaseFirst = resolve; });
      events.push("first:end");
    });
    const second = withGlobalAdmissionLock(async () => {
      events.push("second:start");
    });
    await Promise.resolve();
    await Promise.resolve();
    expect(events).toEqual(["first:start"]);
    releaseFirst();
    await Promise.all([first, second]);
    expect(events).toEqual(["first:start", "first:end", "second:start"]);
  });

  it("lets nested admission inside the holder proceed instead of deadlocking", async () => {
    const result = await withGlobalAdmissionLock(() => withGlobalAdmissionLock(async () => "nested"));
    expect(result).toBe("nested");
  });

  it("releases the lock when a holder throws", async () => {
    await expect(withGlobalAdmissionLock(async () => { throw new Error("claim failed"); })).rejects.toThrow("claim failed");
    await expect(withGlobalAdmissionLock(async () => "next")).resolves.toBe("next");
  });
});
