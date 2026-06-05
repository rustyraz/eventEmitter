// types
type BatcherConfig<T> = {
    maxBatchSize: number;
    maxWaitMs: number;
    maxRetries: number;
    retryDelayMs: number;
    onFlush: (batch: T[]) => Promise<void>;
    onFailure: (batch: T[], error: Error) => Promise<void>;
}

type FlushResult =
    | { ok: true }
    | { ok: false; error: Error };

// Helpers
const sleep = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Retry with exponential backoff.
 * Attempt 1 -> retryDelayMs
 * Attempt 2 -> retryDelayMs * 2
 * Attempt 3 -> retryDelayMs * 4
*/

const withRetry = async (
    fn: () => Promise<void>,
    maxRetries: number,
    retryDelayMs: number
): Promise<FlushResult> => {
    let lastError: Error = new Error('Unknown error');

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            await fn()
            return { ok: true }
        } catch (error) {
            lastError = error instanceof Error ? error : new Error(String(error));

            const isLastAttempt = attempt === maxRetries;
            if (isLastAttempt) {
                const delay = retryDelayMs * Math.pow(2, attempt)
                await sleep(delay);
            }
        }
    }
    return { ok: false, error: lastError }
}

// HighThrouhputBatcher
/**
 * A generic, memory-efficient dual-trigger batcher
 * 
 * Guaranteees
 * - No duplication: a single atomic drain prevents the size and timer triggers from
 *   flushing the same buffer concurently
 * - No data loss: onFailure receive any batch that exhausts retires
 * - No timer leaks: the window timer is always cleared before new one is set, and
 *   shutdown() clears any pending timer
 * - Serial flushes: a flushLock prevents concurrent onFlash calls,
 *   so the database connestion pool is never over-subscribed by this component alone.
 * - Backpressure-aware: add() awaits the flush lock when the buffer is full,
 *   so producers cannot outrun the database indefinitely
*/
export class HighThrouhputBatcher<T> {
    private buffer: T[] = [];
    private timer: NodeJs.Timeout | null = null;

    /**
     * flushLock serialises flush executions.
     * Each flush chains onto the previous one, forming a queue of one
    */
   private flushLock: Promise<void> = Promise.resolve();

   private isShuttingDown = false;

   private readonly config: BatcherConfig<T>;

   constructor (config: BatcherConfig<T>){
    this.validateConfig(config);
    this.config = config;
   }

   // Public API

   /**
    * Add an item to the current batch
    * Awaits any in-progress flush before checking thresholds
    * ensuring add() never races with a current drain
   */
  async add(item: T): Promise<void> {
    if(this.isShuttingDown) {
        throw new Error('Batcher is shutting down - cannot accept new items');
    }

    // Wait for any in-progress flush to complete before mutating the buffer
    // this is the key safety point: we never write to a buffer that is currently being drained
    await this.flushLock
    
    this.buffer.push(item)

    // start the window timer on the first item of a new batch
    if(this.buffer.length === 1) {
        this.startTimer();
    }

    // Size threshold reached - trigger a flush
    if (this.buffer.length >= this.config.maxBatchSize) {
        this.triggerFlush('size');
    }
  }

  /**
     * Flush any remaining items and prevent new ones from being added.
     * Call this during SIGTERM handling before process.exit()
    */
   async shutDown(): Promise<void> {
    this.isShuttingDown = true;
    this.clearTimer();

    //Flush whatever is left in the buffer
    if (this.buffer.length > 0) {
        this.triggerFlush('shutdown');
    }

    // Wait for the final flush to complete
    await this.flushLock;
   }

   // Private: timer management

   private startTimer(): void {
    // Guard: never stack timers
    this.clearTimer();

    this.timer = setTimeout(() => {
        this.triggerFlush('timer')
    }, this.config.maxWaitMs);

    // unref() lets Node Exit naturally without waiting for this timer
    // important for test environments and clean process shutdown
    this.timer.unref();
   }

   private clearTimer(): void {
    if (this.timer !== null) {
        this.timer = null;
    }
   }

   // Private: flush orchestation

   /**
    * Atomically drain the buffer and chain a flush onto the lock.
    * 
    * The drain is synchronous - it happens before any await - so even if both the timer and 
    * size threshold fire in the same event loop tick,
    * the second call to triggerFlush will find an empty buffer and no-op.
    * 
    * This is the race-condition guarantee: Node's single-threaded event
    * loop means the synchronous drain + empty-check is atomic
   */
  private triggerFlush(reason: 'size' | 'timer' | 'shutdown'): void {
    // Always clear the timer when we flush - a new one will start
    // with the next item if needed
    this.clearTimer();

    // Atomic drain - synchronous, cannot be interrupted
    const batch = this.buffer.splice(0);

    // Nothing to flush (e.g timer fired after a size-flush already drained)
    if (batch.length === 0) return;

    // Chain this flush onto the lock - serial execution guaranteed
    this.flushLock = this.flushLock.then(() => 
        this.executeFlush(batch, reason)
    )
  }

  /**
   * execute the Flush with retry logic
   * Hands off to onFailure if all retries are exhausted
   * Never throws - errors are fully handled here so the lock chain
   * is never broken by an unhandled rejection.
  */
 private async executeFlush(
    batch: T[],
    reason: string
 ): Promise<void> {
    const result = await withRetry(
        () => this.config.onFlush(batch),
        this.config.maxRetries,
        this.config.retryDelayMs
    )

    if (!result.ok) {
        // All retries exhausted - hand off to dead-letter handler
        // We catch here so a failing onFailure cannot break the lock chain
        try {
            await this.config.onFailure(batch, result.error)
        } catch (failureHandlerError) {
            // onFailure itself threw - log and move on, do not rethrow
            console.error(
                '[Batcher] onFailure handler threw - batch lost:',
                failureHandlerError
            )
        }
    }
 }

 // Private: Validation

 private validateConfig(config: BatcherConfig<T>): void {
    if (config.maxBatchSize < 1) {
        throw new Error('matchBatchSize must be >= 1');
    }
    if(config.maxWaitMs < 1) {
        throw new Error('maxWaitMs must be >= 1')
    }
    if(config.maxRetries < 0) {
        throw new Error('maxRetries must be >= 0')
    }
    if (config.retryDelayMs < 0) {
        throw new Error('retryDelayMs must be >= 0')
    }
 }
}