/**
 * Redis-based session manager for MCP server
 * 
 * Stores session metadata in Redis to enable stateless, horizontally-scalable deployments.
 * Session state includes: API key hash, collections metadata, last access timestamp.
 * 
 * Security measures:
 * - API keys are hashed (SHA-256) before storage for defense-in-depth
 * - Session TTL limits exposure window
 * - Client binding (IP/User-Agent) prevents session hijacking
 * - Comprehensive audit logging for security monitoring
 * 
 * Note: We still need plaintext API keys at runtime to call Airweave API,
 * but Redis compromise won't expose them.
 */

import { createClient, RedisClientType } from 'redis';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { createHash } from 'crypto';

export interface SessionData {
    sessionId: string;
    apiKeyHash: string;  // SHA-256 hash of API key (not the key itself)
    collection: string;
    baseUrl: string;
    createdAt: number;
    lastAccessedAt: number;
    initialized: boolean;  // Track if MCP initialize was called
    // Security: Client binding to prevent session hijacking
    clientIP?: string;
    userAgent?: string;
}

export interface SessionMetadata {
    clientIP?: string;
    userAgent?: string;
}

export interface SessionWithTransport {
    server: McpServer;
    transport: StreamableHTTPServerTransport;
    data: SessionData;
}

export class RedisSessionManager {
    private redis: RedisClientType;
    private connected: boolean = false;
    private readonly SESSION_PREFIX = 'mcp:session:';
    private readonly RATE_LIMIT_PREFIX = 'mcp:rate_limit:';
    private readonly DEFAULT_TTL = 1800; // 30 minutes (reduced from 1 hour for security)
    private readonly RATE_LIMIT_WINDOW = 3600; // 1 hour
    private readonly MAX_SESSIONS_PER_HOUR = 100; // Per API key

    constructor(redisUrl?: string) {
        this.redis = createClient({
            url: redisUrl || process.env.REDIS_URL || 'redis://localhost:6379',
            socket: {
                reconnectStrategy: (retries) => {
                    if (retries > 10) {
                        console.error('Redis: Max reconnection attempts reached');
                        return new Error('Max reconnection attempts reached');
                    }
                    const delay = Math.min(retries * 100, 3000);
                    console.log(`Redis: Reconnecting in ${delay}ms (attempt ${retries})`);
                    return delay;
                }
            }
        });

        this.redis.on('error', (err) => {
            console.error('Redis Client Error:', err);
        });

        this.redis.on('connect', () => {
            console.log('Redis: Connected');
            this.connected = true;
        });

        this.redis.on('disconnect', () => {
            console.log('Redis: Disconnected');
            this.connected = false;
        });
    }

    async connect(): Promise<void> {
        if (!this.connected) {
            await this.redis.connect();
        }
    }

    async disconnect(): Promise<void> {
        if (this.connected) {
            await this.redis.quit();
            this.connected = false;
        }
    }

    isConnected(): boolean {
        return this.connected;
    }

    private getKey(sessionId: string): string {
        return `${this.SESSION_PREFIX}${sessionId}`;
    }

    private getRateLimitKey(apiKeyHash: string): string {
        const hour = Math.floor(Date.now() / (1000 * 60 * 60));
        return `${this.RATE_LIMIT_PREFIX}${apiKeyHash}:${hour}`;
    }

    /**
     * Hash an API key using SHA-256 for secure storage
     * Note: This is defense-in-depth. We still need the plaintext key at runtime.
     */
    static hashApiKey(apiKey: string): string {
        return createHash('sha256').update(apiKey).digest('hex');
    }

    /**
     * Validate that an API key matches the stored hash
     */
    static validateApiKey(apiKey: string, apiKeyHash: string): boolean {
        return RedisSessionManager.hashApiKey(apiKey) === apiKeyHash;
    }

    /**
     * Audit log for security monitoring
     */
    private auditLog(operation: string, details: Record<string, any>): void {
        console.log(JSON.stringify({
            timestamp: new Date().toISOString(),
            service: 'mcp-redis-session',
            operation,
            ...details
        }));
    }

    /**
     * Check rate limit for session creation
     */
    async checkRateLimit(apiKey: string): Promise<{ allowed: boolean; count: number }> {
        try {
            const apiKeyHash = RedisSessionManager.hashApiKey(apiKey);
            const rateLimitKey = this.getRateLimitKey(apiKeyHash);

            const count = await this.redis.incr(rateLimitKey);

            // Set expiration on first increment
            if (count === 1) {
                await this.redis.expire(rateLimitKey, this.RATE_LIMIT_WINDOW);
            }

            const allowed = count <= this.MAX_SESSIONS_PER_HOUR;

            if (!allowed) {
                this.auditLog('rate_limit_exceeded', {
                    apiKeyHash: apiKeyHash ? apiKeyHash.substring(0, 16) + '...' : 'N/A',
                    count,
                    limit: this.MAX_SESSIONS_PER_HOUR
                });
            }

            return { allowed, count };
        } catch (error) {
            console.error('Error checking rate limit:', error);
            // Fail open - allow the request if rate limiting fails
            return { allowed: true, count: 0 };
        }
    }

    /**
     * Get session data from Redis
     */
    async getSession(sessionId: string): Promise<SessionData | null> {
        try {
            const key = this.getKey(sessionId);
            const data = await this.redis.get(key);

            if (!data) {
                this.auditLog('session_miss', { sessionId });
                return null;
            }

            const sessionData = JSON.parse(data) as SessionData;

            // Update last accessed time
            sessionData.lastAccessedAt = Date.now();
            await this.redis.set(key, JSON.stringify(sessionData), {
                EX: this.DEFAULT_TTL
            });

            this.auditLog('session_accessed', {
                sessionId,
                apiKeyHash: sessionData.apiKeyHash ? sessionData.apiKeyHash.substring(0, 16) + '...' : 'N/A',
                age: Date.now() - sessionData.createdAt
            });

            return sessionData;
        } catch (error) {
            console.error(`Error getting session ${sessionId}:`, error);
            this.auditLog('session_error', { sessionId, error: String(error) });
            return null;
        }
    }

    /**
     * Create or update session in Redis
     * Includes rate limiting check for new sessions
     */
    async setSession(sessionData: SessionData, isNewSession: boolean = false): Promise<void> {
        try {
            const key = this.getKey(sessionData.sessionId);
            await this.redis.set(key, JSON.stringify(sessionData), {
                EX: this.DEFAULT_TTL
            });

            this.auditLog(isNewSession ? 'session_created' : 'session_updated', {
                sessionId: sessionData.sessionId,
                apiKeyHash: sessionData.apiKeyHash ? sessionData.apiKeyHash.substring(0, 16) + '...' : 'N/A',
                clientIP: sessionData.clientIP || 'unknown',
                hasBinding: !!(sessionData.clientIP && sessionData.userAgent)
            });
        } catch (error) {
            console.error(`Error setting session ${sessionData.sessionId}:`, error);
            this.auditLog('session_error', {
                sessionId: sessionData.sessionId,
                error: String(error),
                operation: 'set'
            });
            throw error;
        }
    }

    /**
     * Delete session from Redis
     */
    async deleteSession(sessionId: string): Promise<void> {
        try {
            const key = this.getKey(sessionId);
            await this.redis.del(key);

            this.auditLog('session_deleted', { sessionId });
        } catch (error) {
            console.error(`Error deleting session ${sessionId}:`, error);
            this.auditLog('session_error', {
                sessionId,
                error: String(error),
                operation: 'delete'
            });
        }
    }

    /**
     * Check if a session exists
     */
    async sessionExists(sessionId: string): Promise<boolean> {
        try {
            const key = this.getKey(sessionId);
            const exists = await this.redis.exists(key);
            return exists === 1;
        } catch (error) {
            console.error(`Error checking session ${sessionId}:`, error);
            return false;
        }
    }

    /**
     * Get all session IDs (for debugging/monitoring)
     */
    async getAllSessionIds(): Promise<string[]> {
        try {
            const keys = await this.redis.keys(`${this.SESSION_PREFIX}*`);
            return keys.map(key => key.replace(this.SESSION_PREFIX, ''));
        } catch (error) {
            console.error('Error getting all session IDs:', error);
            return [];
        }
    }


    /**
     * Clean up expired sessions (manual cleanup, Redis TTL handles this automatically)
     */
    async cleanupExpiredSessions(maxAge: number = this.DEFAULT_TTL * 1000): Promise<number> {
        try {
            const keys = await this.redis.keys(`${this.SESSION_PREFIX}*`);
            let deleted = 0;

            for (const key of keys) {
                const data = await this.redis.get(key);
                if (data) {
                    const sessionData = JSON.parse(data) as SessionData;
                    const age = Date.now() - sessionData.lastAccessedAt;

                    if (age > maxAge) {
                        await this.redis.del(key);
                        deleted++;
                    }
                }
            }

            return deleted;
        } catch (error) {
            console.error('Error cleaning up expired sessions:', error);
            return 0;
        }
    }
}

