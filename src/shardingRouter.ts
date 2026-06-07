import { createHash } from 'node:crypto';

// Types and Interfaces

export interface PropertyListing {
    id: string;
    postalCode: string;
    title: string;
    price: number;
}

export interface DatabasePool {
    query<T>(sql: string, params: any[]): Promise<T[]>;
    execute(sql: string, params: any[]): Promise<void>;
}

export interface ListingRepository {
    findbyId(id: string): Promise<PropertyListing | null>;
    save(listing: PropertyListing): Promise<void>;
    findByPostalCode(postalCode: string): Promise<PropertyListing[]>;
}

interface RingNode {
    hash: number;
    shardId: string;
}

// Deterministic Hash Engine

/**
 * Hashed a string key uniformly into an unsigned 32-bit integer space (0 to 4,294,967,295 )
 * Uses SHA-256 for excellent avalanche properties, ensuring uniform data distribution
*/
export function hashTo32BitInt(key: string): number {
    const hashBuffer = createHash('sha256').update(key).digest();
    // Read the first 4 bytes as an unsigned 32-bit Big-Endian Integer
    return hashBuffer.readUInt32BE(0);
}

// Consistent Hash Ring Router

export class ShardRouter {
    private ring: RingNode[] =  [];
    private readonly vnodeCount: number;

    /**
     * @params shardIds Array of physical shard identifiers (['shard-us-1', 'shard-us-2'])
     * @params vnodePerShard Number of virtual positions per physical shard to balance distribution
    */
   constructor(shardIds: string[], vnodesPerShard = 100){
    if (shardIds.length === 0) {
        throw new Error('[Router] Cannot initialize a hash ring with zero shards');
    }
    this.vnodeCount = vnodesPerShard;
    this.initializeRing(shardIds);
   }

   /**
    * Builds the sorted hahs ring allocating virtual nodes for each shard
   */
  private initializeRing(shardIds: string[]): void {
    for (const shardId of shardIds) {
        for (let i = 0; i < this.vnodeCount; i++) {
            //generate a distinct string descriptor for each virtual node
            const vnodeKey = `${shardId}#vnode-${i}`;
            const hash = hashTo32BitInt(vnodeKey);
            this.ring.push({ hash, shardId });
        }
    }
    // sort the ring ascending by hash value to allow Binary search lookups
    this.ring.sort((a,b) => a.hash - b.hash);
  }

  /**
   * Routes a routing key (e.g listingId or postalCode) to its assigned physical shard ID
   * by traversing clockwise along the hash ring
  */
 public getShardId(routingKey: string): string {
    const keyHash = hashTo32BitInt(routingKey);

    // Binary search (O(log M)) where M = physical_shards * vnodeCount
    let low = 0;
    let high = this.ring.length - 1;

    // if the hash is greater than the largest node in the ring, wrap around to index 0
    if (keyHash > this.ring[high].hash) {
        return this.ring[0]?.shardId
    }

    let resultIndex = 0;
    while (low <= high) {
        const mid = Math.floor((low + high)/2);
        if (this.ring[mid]?.hash >= keyHash) {
            resultIndex = mid;
            high = mid - 1; // Look for a closer point clockwise
        } else {
            low = mid + 1;
        }
    }

    return this.ring[resultIndex]?.shardId;
 }
}

// The Sharded Repository Layer

export class ShardedLisitingRepository implements ListingRepository {
    /**
     * Injecting the ShardRouter alongside our decoupled database connection pools
    */
   constructor (
    private readonly router: ShardRouter,
    private readonly shardPools: Map<string, DatabasePool>
   ) {}

   public async findbyId(id: string): Promise<PropertyListing | null> {
       // 1. Determine whish shard owns this specific listing ID
       const shardId = this.router.getShardId(id);
       const pool = this.getPool(shardId);

       // 2. Query only that specific database pool
       const results = await pool.query<PropertyListing>(
        'SELECT is, postal_code as "postalCode", title, price FROM listings WHERE id = $1',
        [id]
       );

       return results[0] ?? null;
   }

   public async save(listing: PropertyListing): Promise<void> {
       // Shard routing key selection determines data placement policy
       // Sharding on 'id' guarantees even write spreads
       const shardId = this.router.getShardId(listing.id);
       const pool = this.getPool(shardId);

       await pool.execute(
        `INSERT INTO listings (id, postal_code, title, price)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO UPDATE SET title = $3, price = $4`,
        [listing.id, listing.postalCode, listing.title, listing.price]
       );
   }

   /**
     * Advanced Architectural Demonstration: Scatter-Gather Pattern
     * * If we shard our cluster by 'id', querying by a non-sharding key like 'postalCode' 
     * means we don't know where the records live. We must fan out queries to ALL shards 
     * concurrently and merge the results.
   */
  public async findByPostalCode(postalCode: string): Promise<PropertyListing[]> {
      const promises: Promise<PropertyListing[]>[] = [];

      // Query every single shard connection pool concurrently
      for (const [shardId, pool] of this.shardPools.entries()) {
        const shardQuery = pool.query<PropertyListing>(
            'SELECT id, postal_code as "postalCode", title, price FROM listings WHERE postal_code = $1',
            [postalCode]
        ).catch(error => {
            // Protect the aggregation loop from single shard infrastructure failure
            console.error(`[Repository] Shard query failed on ${shardId}`, error);
            return [] as PropertyListing[];
        });
        promises.push(shardQuery);
      }

      // Await all pools, then flatten the results matrix
      const resultsMatrix = await Promise.all(promises);
      return resultsMatrix.flat();
  }

  private getPool(shardId: string): DatabasePool {
    const pool = this.shardPools.get(shardId);
    if(!pool) {
        throw new Error(`[Repository] Infrastructure error: No connection pool configured for shard: ${shardId} `);
    }
    return pool;
  }
}

