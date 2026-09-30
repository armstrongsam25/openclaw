import { toErrorObject } from "@openclaw/normalization-core/error-coercion";
import { createDeferredCore } from "../shared/deferred.js";
import type {
  EnqueueOptions,
  RequestBody,
  Slot,
  SqliteWorkerInputPreparation,
  SqliteWorkerInputRetention,
} from "./sqlite-worker-broker.types.js";
import { SqliteWorkerError } from "./sqlite-worker-contract.js";

type RetainedOpen = {
  start(settle: () => void): void;
  service(dependent: boolean): void;
  enterDependency?: (operation: () => void) => void;
};

/** Retained inputs share the broker budget before they become queued jobs. */
export class SqliteWorkerInputAdmission {
  private bytes = 0;
  private inputPreparationGeneration = {};
  private readonly inputPreparations = new Set<Promise<void>>();
  private openTail: Promise<void> = Promise.resolve();
  private readonly queuedOpens: RetainedOpen[] = [];
  private activeOpen?: RetainedOpen;
  private advancingOpens = false;

  constructor(
    private readonly owner: {
      queuedBytes(): number;
      isClosing(): boolean;
      maxQueuedBytes: number;
      maxQueuedInputBytes: number;
      maxMessageBytes: number;
    },
  ) {}

  get opensPending(): boolean {
    return this.activeOpen !== undefined || this.queuedOpens.length > 0;
  }

  get preparationsPending(): boolean {
    return this.inputPreparations.size > 0;
  }

  get retainedBytes(): number {
    return this.bytes;
  }

  reserveOpenInput(bytes: number): SqliteWorkerInputPreparation {
    if (
      bytes > this.owner.maxMessageBytes ||
      this.owner.queuedBytes() + this.bytes + bytes > this.owner.maxQueuedBytes
    ) {
      throw new SqliteWorkerError("SQLite worker open input capacity reached", "overloaded");
    }
    return this.reserveInputPreparation(bytes, "snapshot");
  }

  retain(bytes: number): () => void {
    this.bytes += bytes;
    let released = false;
    return () => {
      if (!released) {
        released = true;
        this.bytes -= bytes;
      }
    };
  }

  /** Physical identity publication ends this same FIFO turn, independent of Promise delivery. */
  startOpen(
    start: RetainedOpen["start"],
    service: RetainedOpen["service"],
    enterDependency?: RetainedOpen["enterDependency"],
  ): () => void {
    const settled = createDeferredCore();
    this.openTail = settled.promise;
    const retained: RetainedOpen = {
      start: (finish) =>
        start(() => {
          settled.resolve();
          finish();
        }),
      service,
      enterDependency,
    };
    this.queuedOpens.push(retained);
    this.advanceOpens();
    return () => this.serviceOpen(retained);
  }

  serviceOpens(): void {
    this.serviceOpen();
  }

  private serviceOpen(dependent?: RetainedOpen): void {
    this.advanceOpens();
    const active = this.activeOpen;
    if (active && dependent?.enterDependency && this.queuedOpens.includes(dependent)) {
      dependent.enterDependency(() => active.service(true));
    } else {
      active?.service(false);
    }
    this.advanceOpens();
  }

  private advanceOpens(): void {
    if (this.advancingOpens) {
      return;
    }
    this.advancingOpens = true;
    try {
      while (!this.activeOpen) {
        const retained = this.queuedOpens.shift();
        if (!retained) {
          return;
        }
        this.activeOpen = retained;
        retained.start(() => {
          if (this.activeOpen === retained) {
            this.activeOpen = undefined;
            this.advanceOpens();
          }
        });
      }
    } finally {
      this.advancingOpens = false;
    }
  }

  joinOpens(): Promise<void> {
    return this.openTail;
  }

  invalidatePreparations(): void {
    this.inputPreparationGeneration = {};
  }

  async joinPreparations(): Promise<void> {
    await Promise.allSettled(this.inputPreparations);
  }

  reserveInputPreparation(
    inputBytes: number,
    retention: SqliteWorkerInputRetention = "stream",
  ): SqliteWorkerInputPreparation {
    if (!Number.isSafeInteger(inputBytes) || inputBytes < 0) {
      throw new RangeError("SQLite worker input bytes must be a non-negative safe integer");
    }
    if (this.owner.isClosing()) {
      throw new SqliteWorkerError("SQLite worker host is closing", "closed");
    }
    const bytes =
      retention === "stream" && inputBytes > this.owner.maxQueuedInputBytes
        ? this.owner.maxMessageBytes
        : inputBytes;
    if (this.owner.queuedBytes() + this.bytes + bytes > this.owner.maxQueuedBytes) {
      throw new SqliteWorkerError("SQLite worker input preparation capacity reached", "overloaded");
    }
    const generation = this.inputPreparationGeneration;
    this.bytes += bytes;
    const settled = createDeferredCore();
    this.inputPreparations.add(settled.promise);
    let released = false;
    let handedOff = false;
    const release = () => {
      if (!released) {
        released = true;
        this.bytes -= bytes;
        this.inputPreparations.delete(settled.promise);
        settled.resolve();
      }
    };
    const assertCurrent = () => {
      if (
        !handedOff &&
        (released || this.owner.isClosing() || generation !== this.inputPreparationGeneration)
      ) {
        throw new SqliteWorkerError("SQLite worker input preparation is closed", "closed");
      }
    };
    return {
      assertCurrent,
      handoff: (dispatch) => {
        try {
          if (released) {
            throw new SqliteWorkerError("SQLite worker input preparation is closed", "closed");
          }
          assertCurrent();
        } catch (error) {
          release();
          throw error;
        }
        // Enqueue takes custody in this same synchronous turn, including its oversized checks.
        handedOff = true;
        release();
        return dispatch();
      },
      release,
    };
  }
}

export function waitForSqliteWorkerCapacity(
  slot: Slot,
  body: RequestBody,
  bytes: number,
  options: EnqueueOptions,
  owner: {
    waiters: Map<Slot, Set<(error?: unknown) => void>>;
    admissionTimeoutMs: number;
    maxRequestsPerWorker: number;
    retain(bytes: number): () => void;
    warnAdmission(slot: Slot, waitMs: number): void;
    enqueue(
      slot: Slot,
      body: RequestBody,
      bytes: number,
      options: EnqueueOptions,
      admitted: boolean,
    ): Promise<unknown>;
  },
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const signal = options.signal;
    const waiters = owner.waiters.get(slot) ?? new Set<(error?: unknown) => void>();
    const resume = (error?: unknown) => {
      if (!waiters.delete(resume)) {
        return;
      }
      if (!waiters.size) {
        owner.waiters.delete(slot);
      }
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      releaseInput();
      let failure = error;
      if (failure === undefined && Date.now() - started >= owner.admissionTimeoutMs) {
        owner.warnAdmission(slot, Date.now() - started);
        failure = new SqliteWorkerError("SQLite worker queue capacity reached", "overloaded");
      }
      if (failure !== undefined) {
        const admissionError = toErrorObject(failure, "SQLite worker admission failed");
        options.settled?.(
          { status: "rejected", error: admissionError },
          { settlement: { kind: "not-entered", error: admissionError } },
        );
        reject(admissionError);
      } else {
        resolve(owner.enqueue(slot, body, bytes, options, true));
      }
    };
    const abort = () => resume(signal?.reason ?? new Error("SQLite worker operation canceled"));
    const timer = setTimeout(() => {
      owner.warnAdmission(slot, Date.now() - started);
      resume(new SqliteWorkerError("SQLite worker queue capacity reached", "overloaded"));
    }, owner.admissionTimeoutMs);
    const releaseInput = owner.retain(bytes);
    waiters.add(resume);
    owner.waiters.set(slot, waiters);
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) {
      abort();
    } else if (waiters.size >= owner.maxRequestsPerWorker) {
      owner.warnAdmission(slot, 0);
    }
  });
}
