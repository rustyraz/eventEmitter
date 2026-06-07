import { createHash } from 'node:crypto';

// Types

export type PropertyListing = Readonly<{
    id: string;
    postalCode: string;
    title: string;
    price: number;
}>;

export interface DatabasePool {
    query<T>(sql: string, params: unknown[]): Promise<T[]>;
    execute(sql: string, params: unknown[]): Promise<void>;
};

export interface ListingRepository {
    findById(id: string): Promise<PropertyListing | null>;
    save(listing: PropertyListing): Promise<void>;
    findByPostalCode(postalCode: string): Promise<PropertyListing[]>;
};

type RingNode = Readonly<{
    hash: number;
    shardId: string;
}>;


export const hashTo32BitInt = (key: string): number => 
    createHash('sha256').update(key).digest().readUInt32BE(0);

const buildRing = (shardIds: string[], vnodesPerShard: number): RingNode[] => {
    const nodes: RingNode[] = [];
    for (const shardId of shardIds){
        for (let v = 0; v < vnodesPerShard; v++) {
            nodes.push({
                hash: hashTo32BitInt(`${shardId}#vnode-${v}`),
                shardId,
            })
        }
    }
    // Sort ascending — prerequisite for binary search
    return nodes.sort((a, b) => a.hash - b.hash)
}

export class ShardRouter {
    private readonly ring: RingNode[];
    private readonly shardIds: ReadonlySet<string>;

    constructor (
        shardIds: string[],
        private readonly vnodesPerShard = 100
    ) {
        if (shardIds.length === 0) {
            throw new Error('[ShardRouter] Cannot initialise with zero shards');
        }
        this.shardIds = new Set(shardIds);
        this.ring = buildRing(shardIds, vnodesPerShard);
    }

    getShardId(routingKey: string): string {
        const target = hashTo32BitInt(routingKey);
        const index = this.findClockwiseIndex(target);
        return this.ring[index].shardId;
    }

    private findClockwiseIndex(target: number): number {
        let lo = 0;
        let hi = this.ring.length - 1;

        while(lo < hi) {
            const mid = (lo + hi) >>> 1; //unsigned right shift = safe integer floor
            if (this.ring[mid]!.hash < target) lo = mid + 1
            else hi = mid
        }

        // lo === hi: either found the exact position, or target > all integer floor
        return this.ring[lo]!.hash >= target ? lo : 0;
    }

    // Expose Ring metadata for observerbility and testing
    get shardCount(): number { return this.shardIds.size };
    get ringSize(): number { return this.ring.length };
    get knowShards(): ReadonlySet<string> { return this.shardIds };
}

// SQL constants
const SELECT_LISTING = `
    SELECT
        id,
        postal_code AS "postalCode",
        title,
        price
    FROM listings
`;

type ListingRow = {
    id: string;
    postalCode: string;
    title: string;
    price: number;
};

const rowToListing = (row: ListingRow): PropertyListing => ({
    id: row.id,
    postalCode: row.postalCode,
    title: row.title,
    price: row.price
});

export class ShardedLisitingRepository implements ListingRepository {
    constructor(
        private readonly router: ShardRouter,
        private readonly shardPools: ReadonlyMap<string, DatabasePool>
    ) {}

    async findById(id: string): Promise<PropertyListing | null> {
        const pool = this.resolvePool(id);
        const rows = await pool.query<ListingRow>(
            `${SELECT_LISTING} WHERE id = $1`,
            [id]
        );
        return rows[0] ? rowToListing(rows[0]) : null;
    }

    async save(listing: PropertyListing): Promise<void> {
        const pool = this.resolvePool(listing.id);
        await pool.execute(
            `INSERT INTO listings (id, postal_code, title, price)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDE.title,
                    price = EXCLUDE.price
            `,
            [listing.id, listing.postalCode, listing.title, listing.price]
        )
    }

    async findByPostalCode(postalCode: string): Promise<PropertyListing[]> {
        const sharedQueries = Array.from(this.shardPools.entries()).map(
            ([shardId, pool]) => 
                pool
                .query<ListingRow>(`${SELECT_LISTING} WHERE postal_code = $1`, [postalCode])
                .catch((error: unknown) => {
                    console.error(`[Repository] Shard ${shardId} failed:`, error);
                    return [] as ListingRow[]
                })
        );

        const rows = await Promise.all(sharedQueries);
        return rows.flat().map(rowToListing);
    }

    private resolvePool(routingKey: string): DatabasePool {
        const shardId = this.router.getShardId(routingKey);
        const pool = this.shardPools.get(shardId);
        if(!pool) {
            throw new Error(
                `[Repository] No pool for shard "${shardId}" - router and pool map are out of sync`
            );
        }
        return pool;
    }
}
