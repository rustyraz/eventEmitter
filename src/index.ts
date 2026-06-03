import { EventEmitter } from 'events';

type ListingStatus = 'active' | 'sold' | 'expired';

type Listing =  {
    id: string;
    agentId: string;
    price: number;
    status: ListingStatus;
    createdAt: Date;
};

type Agent = {
    id: string;
    name: string;
    email: string;
    tier: 'standard' | 'premium';
};

type EnrichedListing = Listing & { agent: Agent };

type PipelineStats = {
    processed: number;
    agentCacheSize: number;
};

const parseListingEvent = (raw: string): Listing | null => {
    try {
        const parsed = JSON.parse(raw);
        if(!parsed || typeof parsed !== 'object') {
            return null;
        }

        if(
            typeof parsed.id === 'string' &&
            typeof parsed.agentId === 'string' &&
            typeof parsed.price === 'number' &&
            typeof parsed.status === 'string'
        ){
            return parsed as Listing;
        }
        return null;
    } catch {
        return null;    }
}

const fetchAgentFromDb = async (id: string): Promise<Agent | null> => {
    const result = await db.query<Agent>(
        `SELECT id, name, email, tier
        FROM agents
        WHERE id = $1`,
        [id]
        
    );
    return result.row[0] ?? null;
}

class ListingPipeline extends EventEmitter {
    private agentCache = new Map<string, Agent>;
    private readonly MAX_CACHE_SIZE = 1000;
    private processed = 0;

    private cacheAgent(id:string, agent: Agent): void {
        if (this.agentCache.size >= this.MAX_CACHE_SIZE) {
            const firstKey = this.agentCache.keys().next().value;
            if(firstKey) this.agentCache.delete(firstKey);
        }
        this.agentCache.set(id, agent);
    }

    private async getAgent(id: string): Promise<Agent | null> {
        if (this.agentCache.has(id)) {
            return this.agentCache.get(id);
        }
        const agent = await fetchAgentFromDb(id);
        if (agent) this.cacheAgent(id, agent);
        return agent;
    }

    async processEvent(raw: string): Promise<EnrichedListing | null> {
        const listing = parseListingEvent(raw);
        
        if (!listing || listing.status !== 'active') return null;

        const agent = await this.getAgent(listing.agentId);
        if(!agent) return null;

        const enriched: EnrichedListing = { ...listing, agent };
        this.processed++
        this.emit('processed', enriched);

        return enriched;
    }


    async processBatch(events: string[]): Promise<EnrichedListing[]> {
        //parallel - all events processed concurrently
        const result = await Promise.all(
            events.map(event => this.processEvent(event))
        );

        // filter out nulls with a type guard - result is EnrichedListing[]
        return result.filter((r): r is EnrichedListing => r !== null)
    }
}
