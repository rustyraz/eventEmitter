/**
 * High-Throughput Event processing and Batching
 * To save round trips to a Postgress shards database, we can write a batching queue that
 * accummulates updated and flushes them to the datastore either when it reaches X items
 * or when a timeout of Y milliseconds is reached  
 */
export class IngestionBatcher<T> {
    private queue: T[] = [];
    private timer: NodeJs.timer | null = null;


    constructor(
        private readonly maxBatchSize: number,
        private readonly maxWaitMs: number,
        private readonly onFlush: (batch: T[]) => Promise<void>
    ){}

    public async add(item: T): Promise<void> {
        this.queue.push(item);

        if (this.queue.length >= this.maxBatchSize){
            await this.flush();
        } else if(!this.timer) {
            this.timer = setTimeout(() => this.flush(), this.maxWaitMs)
        }
    }

    public async flush() {
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }

        if (this.queue.length === 0) return;

        const batchProcess = [...this.queue];
        this.queue = [];

        try {
            this.onFlush(batchProcess);
        } catch (error) {
            // re-queue, emit to an error handler or trigger a retry strategy
            console.error('Failed to flush batch', error);
        }
    }
}