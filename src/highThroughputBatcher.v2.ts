type RetryStrategy = (attempt: number, baseDelayMs: number) => number;

export const fixedDelay: RetryStrategy = (_attempt, baseDelayMs) => baseDelayMs;

export const exponentialBackoff: RetryStrategy = (attempt, baseDelayMs) => baseDelayMs * Math.pow(2, attempt);

const sleep = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

type FlushResult = 
    | { ok: true }
    | { ok: false, error: Error };

const withRetry = async (
    fn: () => Promise<void>,
    maxRetries: number,
    retryDelayMs: number,
    strategy: RetryStrategy
): Promise<FlushResult> => {
    let lastError: Error = new Error('Unknown error');
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            await fn()
            return { ok: true };
        } catch (error) {
            lastError = error instanceof Error ? error : new Error(String(error));
            if(attempt < maxRetries) await sleep(strategy(attempt, retryDelayMs));
        }
    }
    return { ok: false, error: lastError };
}

// config

export type BatcherConfig<T> = {
    maxBatchSize: number;
    maxWaitMs: number;
    maxRetries: number;
    retryDelayMs: number;
    retryStrategy: RetryStrategy,
    onFlush: (batch: ReadonlyArray<T>) => Promise<void>;
    onFailure: (batch: ReadonlyArray<T>, error: Error) => Promise<void>; 
}

// HighThroughputBatcher

export class HighThroughputBatcher<T> {
    private buffer: T[] = [];
    private timer: ReturnType<typeof setTimeout> | null = null;
    private inFlight: Set<Promise<void>> = new Set();
    private isShuttingDown = false;
    private readonly strategy: RetryStrategy;

    constructor(private readonly config: BatcherConfig<T>) {
        this.validateConfig(config);
        this.strategy = config.retryStrategy ?? fixedDelay;
    }

    add(item: T): void {
        if(this.isShuttingDown) {
            throw new Error('[Batcher] Shutting down - cannot accept new items');
        }
        this.buffer.push(item);
        if (this.buffer.length === 1) this.startTimer();
        if (this.buffer.length >= this.config.maxBatchSize) this.triggerFlush();
    }

    async shutDown(): Promise<void> {
        if(this.isShuttingDown) {
            await Promise.all(this.inFlight)
            return;
        }
        this.isShuttingDown = true;
        this.clearTimer();
        if (this.buffer.length > 0) this.triggerFlush();
        await Promise.all(this.inFlight);
    }

    get pendingCount(): number { return this.buffer.length }
    get inFlightCount(): number { return this.inFlight.size }

    private startTimer(): void {
        this.clearTimer();
        this.timer = setTimeout(() => {
            this.timer = null;
            this.triggerFlush();
        }, this.config.maxWaitMs);
        this.timer.unref();
    }

    private clearTimer(): void {
        if (this.timer !== null) { clearTimeout(this.timer); this.timer = null }
    }

    private triggerFlush(): void {
        this.clearTimer();
        const batch = this.buffer.splice(0);
        if (batch.length === 0) return;
        const frozenBatch = Object.freeze(batch) as ReadonlyArray<T>;
        const flush = this.executeFlush(frozenBatch);
        this.inFlight.add(flush);
        flush.finally(() => this.inFlight.delete(flush));
    }

    private async executeFlush(batch: ReadonlyArray<T>): Promise<void> {
        const result = await withRetry(
            () => this.config.onFlush(batch),
            this.config.maxRetries,
            this.config.retryDelayMs,
            this.strategy
        );
        if(!result.ok) {
            try {
                await this.config.onFailure(batch, result.error);
            } catch (handleError) {
                console.error('[Batcher] onFailure handler threw:', handleError );
            }
        }
    }

    // Private: Validation

    private validateConfig(config: BatcherConfig<T>): void {
        if (config.maxBatchSize < 1)  throw new Error('[Batcher] maxBatchSize must be >= 1');
        if (config.maxWaitMs < 1)     throw new Error('[Batcher] maxWaitMs must be >= 1');
        if (config.maxRetries < 0)    throw new Error('[Batcher] maxRetries must be >= 0');
        if (config.retryDelayMs < 0)  throw new Error('[Batcher] retryDelayMs must be >= 0');
    }
}