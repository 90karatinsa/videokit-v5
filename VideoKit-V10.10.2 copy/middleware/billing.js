import crypto from 'crypto';
import promClient from 'prom-client';

import { ensureRequestId, sendError } from '../http-error.js';
import { fetchTenantRow, normalizeTenantRow } from '../tenant-schema.js';

const OPERATIONS = [
  { method: 'POST', pattern: /^\/verify$/i, name: '/verify', weight: 1 },
  { method: 'POST', pattern: /^\/stamp$/i, name: '/stamp', weight: 5 },
  { method: 'POST', pattern: /^\/batch\/upload$/i, name: '/batch/upload', weight: 10 },
  { method: 'GET', pattern: /^\/management\/tenants$/i, name: '/management/tenants', billable: false },
  { method: 'POST', pattern: /^\/management\/keys$/i, name: '/management/keys', billable: false },
  {
    method: 'DELETE',
    pattern: /^\/management\/keys\/[^/]+$/i,
    name: '/management/keys/:keyId',
    billable: false,
  },
];

const TOTAL_WEIGHT_FIELD = '__total__';
const TOTAL_COUNT_FIELD = '__total_count__';
const ENDPOINT_WEIGHT_PREFIX = 'op:';
const ENDPOINT_COUNT_PREFIX = 'op_count:';
const DEFAULT_IDEMPOTENCY_TTL_SECONDS = 86_400; // 24 hours

const REDIS_INCREMENT_LUA = `
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local totalWeightField = ARGV[2]
local totalCountField = ARGV[3]
local endpointWeightField = ARGV[4]
local endpointCountField = ARGV[5]
local weightIncrement = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])

if limit >= 0 then
  local currentWeight = tonumber(redis.call('HGET', key, totalWeightField) or '0')
  if currentWeight + weightIncrement > limit then
    local endpointWeight = tonumber(redis.call('HGET', key, endpointWeightField) or '0')
    local currentCount = tonumber(redis.call('HGET', key, totalCountField) or '0')
    local endpointCount = tonumber(redis.call('HGET', key, endpointCountField) or '0')
    return {0, currentWeight, endpointWeight, currentCount, endpointCount}
  end
end

local newTotalWeight = redis.call('HINCRBYFLOAT', key, totalWeightField, weightIncrement)
local newEndpointWeight = redis.call('HINCRBYFLOAT', key, endpointWeightField, weightIncrement)
local newTotalCount = redis.call('HINCRBY', key, totalCountField, 1)
local newEndpointCount = redis.call('HINCRBY', key, endpointCountField, 1)

if ttl and ttl > 0 then
  redis.call('PEXPIRE', key, ttl)
end

return {1, newTotalWeight, newEndpointWeight, newTotalCount, newEndpointCount}
`;

const READ_RATE_LIMIT_LUA = `
local zsetKey = KEYS[1]
local counterKey = KEYS[2]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', zsetKey, 0, now - window)
local current = redis.call('ZCARD', zsetKey)
local retryAfterMs = 0

if limit <= 0 then
  return {1, current, retryAfterMs}
end

if current >= limit then
  local oldest = redis.call('ZRANGE', zsetKey, 0, 0, 'WITHSCORES')
  if #oldest > 0 then
    retryAfterMs = math.max(0, (tonumber(oldest[2]) + window) - now)
  end
  return {0, current, retryAfterMs}
end

local sequence = redis.call('INCR', counterKey)
redis.call('ZADD', zsetKey, now, now .. '-' .. sequence)
redis.call('EXPIRE', zsetKey, ttl)
redis.call('EXPIRE', counterKey, ttl)

current = current + 1

if current >= limit then
  local oldestAfterInsert = redis.call('ZRANGE', zsetKey, 0, 0, 'WITHSCORES')
  if #oldestAfterInsert > 0 then
    retryAfterMs = math.max(0, (tonumber(oldestAfterInsert[2]) + window) - now)
  end
end

return {1, current, retryAfterMs}
`;

const USAGE_THRESHOLDS = [
  { value: 0.8, label: '80' },
  { value: 0.9, label: '90' },
  { value: 1, label: '100' },
];

const READ_RATE_LIMIT_WINDOW_MS_DEFAULT = 60_000;

const state = {
  redis: null,
  dbPool: null,
  now: () => new Date(),
  idempotencyTtlSeconds: DEFAULT_IDEMPOTENCY_TTL_SECONDS,
  planCache: new Map(),
  logger: console,
  readRateLimits: new Map(),
  readRateLimitWindowMs: READ_RATE_LIMIT_WINDOW_MS_DEFAULT,
  thresholdCache: new Map(),
  readLimiterCache: new Map(),
};

const register = promClient.register;

const getOrCreateMetric = (name, factory) => {
  const existing = register.getSingleMetric(name);
  if (existing) {
    return existing;
  }
  return factory();
};

const DEFAULT_TENANT_LABEL = 'unknown';

const histogramName = 'videokit_api_billable_duration_ms';
let durationMetric = register.getSingleMetric(histogramName);
if (!durationMetric) {
  durationMetric = new promClient.Histogram({
    name: histogramName,
    help: 'Duration of billable API calls in milliseconds.',
    labelNames: ['method', 'endpoint', 'status'],
    buckets: [50, 100, 200, 500, 1000, 2000, 5000, 10_000],
  });
}

const counterName = 'videokit_api_billable_requests_total';
let requestCounter = register.getSingleMetric(counterName);
if (!requestCounter) {
  requestCounter = new promClient.Counter({
    name: counterName,
    help: 'Total billable API requests processed.',
    labelNames: ['method', 'endpoint', 'status', 'billable'],
  });
}

const quotaBlockMetricName = 'quota_block_total';
const quotaBlockCounter = getOrCreateMetric(quotaBlockMetricName, () => new promClient.Counter({
  name: quotaBlockMetricName,
  help: 'Toplam kota aşımlarının sayısı.',
  labelNames: ['tenant', 'endpoint'],
}));

const analyticsInsertFailuresName = 'analytics_insert_failures_total';
const analyticsInsertFailuresTotal = getOrCreateMetric(analyticsInsertFailuresName, () => new promClient.Counter({
  name: analyticsInsertFailuresName,
  help: 'API analitik kayıtlarının veritabanına yazılamadığı toplam durum sayısı.',
}));

const normalizeRateLimitMap = (limits) => {
  if (!limits) return new Map();

  if (limits instanceof Map) {
    return new Map(limits.entries());
  }

  const map = new Map();
  for (const [key, value] of Object.entries(limits)) {
    const num = Number(value);
    if (Number.isFinite(num) && num > 0) {
      map.set(key, num);
    }
  }

  return map;
};

export const configureBilling = ({
  redis,
  dbPool,
  now,
  idempotencyTtlSeconds,
  logger,
  readRateLimits,
  readRateLimitWindowMs,
} = {}) => {
  if (redis) state.redis = redis;
  if (dbPool) state.dbPool = dbPool;
  if (typeof now === 'function') state.now = now;
  if (Number.isFinite(idempotencyTtlSeconds) && idempotencyTtlSeconds > 0) {
    state.idempotencyTtlSeconds = idempotencyTtlSeconds;
  }
  if (logger) state.logger = logger;
  if (readRateLimits) {
    state.readRateLimits = normalizeRateLimitMap(readRateLimits);
  }
  if (Number.isFinite(readRateLimitWindowMs) && readRateLimitWindowMs > 0) {
    state.readRateLimitWindowMs = readRateLimitWindowMs;
  }
};

const ensureConfigured = () => {
  if (!state.redis || !state.dbPool) {
    throw new Error('Billing middleware requires configureBilling to run first.');
  }

  return state;
};

const ensureReqState = (req) => {
  if (!req.billing) {
    req.billing = {};
  }
  return req.billing;
};

const normalizePath = (rawPath) => {
  const path = (rawPath || '').split('?')[0].replace(/\\+/g, '/');
  if (!path || path === '/') {
    return '/';
  }
  return path.endsWith('/') ? path.slice(0, -1) || '/' : path;
};

const matchOperation = (req) => {
  const method = (req.method || '').toUpperCase();
  const raw = req.route?.path || req.originalUrl || req.path || '/';
  const normalized = normalizePath(raw);

  for (const operation of OPERATIONS) {
    if (operation.method === method && operation.pattern.test(normalized)) {
      return { ...operation, normalizedEndpoint: operation.name ?? normalized };
    }
  }

  return { method, normalizedEndpoint: normalized, weight: 1, billable: true };
};

const getOperationContext = (req) => {
  const stateForRequest = ensureReqState(req);
  if (!stateForRequest.operation) {
    stateForRequest.operation = matchOperation(req);
  }
  return stateForRequest.operation;
};

const parseJson = (value) => {
  if (!value) return null;
  if (typeof value === 'object') return value;

  try {
    return JSON.parse(value);
  } catch (error) {
    return null;
  }
};

const loadPlan = async (planId, dbPool) => {
  if (!planId) return null;

  const cached = state.planCache.get(planId);
  const now = Date.now();
  if (cached && cached.expiresAt > now) {
    return cached.value;
  }

  const result = await dbPool.query(
    'SELECT plan_id, monthly_api_calls_total, endpoint_overrides FROM plan_entitlements WHERE plan_id = $1',
    [planId],
  );
  const value = result.rows[0] ?? null;
  state.planCache.set(planId, { value, expiresAt: now + 300_000 });
  return value;
};

const loadTenantById = async (tenantId, { redis, dbPool }) => {
  if (!tenantId) return null;

  const cached = redis ? await redis.hgetall(`tenant:${tenantId}`) : null;
  const cachedPlanId = cached?.plan_id ?? cached?.plan ?? null;
  if (cached?.id && cachedPlanId) {
    return {
      id: cached.id,
      name: cached.name,
      planId: cachedPlanId,
      quotaOverride: parseJson(cached.quota_override),
    };
  }

  const row = await fetchTenantRow(dbPool, tenantId, {
    includeName: true,
    includePlan: true,
    includeQuota: true,
    includeTimestamps: false,
  });
  if (!row) return null;

  const normalized = normalizeTenantRow(row);
  const planId = normalized.plan_id ?? normalized.plan ?? null;
  const tenant = {
    id: normalized.id,
    name: normalized.name,
    planId,
    quotaOverride: parseJson(normalized.quota_override),
  };

  if (redis) {
    await redis.hset(`tenant:${tenantId}`, {
      id: tenant.id,
      name: tenant.name ?? '',
      plan_id: planId ?? '',
      plan: planId ?? '',
      quota_override: normalized.quota_override ? JSON.stringify(normalized.quota_override) : '',
    });
    await redis.expire(`tenant:${tenantId}`, 3600);
  }

  return tenant;
};

const resolveTenantIdFromApiKey = async (apiKey, { redis, dbPool }) => {
  if (!apiKey) return null;

  const direct = redis ? await redis.get(`api_key:${apiKey}`) : null;
  if (direct) return direct;

  const hash = crypto.createHash('sha256').update(apiKey).digest('hex');
  if (redis) {
    const cached = await redis.get(`api_key_hash:${hash}`);
    if (cached) return cached;
  }

  try {
    const result = await dbPool.query(
      'SELECT tenant_id FROM api_keys WHERE key_hash = $1 LIMIT 1',
      [hash],
    );
    const tenantId = result.rows[0]?.tenant_id ?? null;
    if (tenantId && redis) {
      await redis.set(`api_key:${apiKey}`, tenantId, 'EX', 3600);
      await redis.set(`api_key_hash:${hash}`, tenantId, 'EX', 3600);
    }
    return tenantId;
  } catch (error) {
    if (error.code !== '42P01') throw error;
    return null;
  }
};

const mergeQuota = (plan, override) => {
  const planLimit = plan?.monthly_api_calls_total ?? null;
  const overrideLimit = override?.monthly_api_calls_total ?? override?.total ?? null;

  const endpointOverrides = { ...(plan?.endpoint_overrides ?? {}) };
  if (override?.endpoint_overrides) {
    for (const [endpoint, value] of Object.entries(override.endpoint_overrides)) {
      endpointOverrides[endpoint] = {
        ...(endpointOverrides[endpoint] ?? {}),
        ...value,
      };
    }
  }

  return { limit: overrideLimit ?? planLimit ?? null, endpointOverrides };
};

const computePeriod = (now) => {
  const year = now.getUTCFullYear();
  const month = now.getUTCMonth();
  const key = `${year}-${String(month + 1).padStart(2, '0')}`;
  const start = new Date(Date.UTC(year, month, 1));
  const next = new Date(Date.UTC(year, month + 1, 1));
  return {
    key,
    start,
    end: new Date(next.getTime() - 1),
    ttlSeconds: Math.max(1, Math.floor((next - now) / 1000)),
  };
};

const endpointKey = (endpoint) => endpoint.replace(/\s+/g, '_');
const endpointWeightField = (endpoint) => `${ENDPOINT_WEIGHT_PREFIX}${endpointKey(endpoint)}`;
const endpointCountField = (endpoint) => `${ENDPOINT_COUNT_PREFIX}${endpointKey(endpoint)}`;

const requestHash = (req, endpoint) => {
  const hash = crypto.createHash('sha256');
  hash.update((req.method || '').toUpperCase());
  hash.update('|');
  hash.update(endpoint || req.originalUrl || '');
  hash.update('|');
  hash.update(req.headers['content-type'] || '');
  hash.update('|');
  hash.update(req.headers['content-length'] || '');
  if (req.body && Object.keys(req.body).length) {
    hash.update(`|${JSON.stringify(req.body)}`);
  }
  return hash.digest('hex');
};

const upsertIdempotency = async (req, endpoint, tenantId) => {
  const key = req.get?.('Idempotency-Key');
  if (!key) {
    return { skipBilling: false };
  }

  const { dbPool } = ensureConfigured();
  const hash = requestHash(req, endpoint);
  const expiresAt = new Date(state.now().getTime() + state.idempotencyTtlSeconds * 1000);

  try {
    const insert = await dbPool.query(
      `INSERT INTO idempotency_keys (idempotency_key, tenant_id, endpoint, request_hash, expires_at, locked_at)
       VALUES ($1, $2, $3, $4, $5, NOW())
       ON CONFLICT DO NOTHING`,
      [key, tenantId, endpoint, hash, expiresAt],
    );

    const select = await dbPool.query(
      'SELECT request_hash, status_code FROM idempotency_keys WHERE idempotency_key = $1',
      [key],
    );
    const row = select.rows[0];
    if (!row) {
      return { key, requestHash: hash, skipBilling: false };
    }

    if (row.request_hash !== hash) {
      const error = new Error('Idempotency hash mismatch');
      error.statusCode = 409;
      throw error;
    }

    await dbPool.query(
      'UPDATE idempotency_keys SET last_accessed_at = NOW(), locked_at = NOW() WHERE idempotency_key = $1',
      [key],
    );

    return {
      key,
      requestHash: hash,
      skipBilling: insert.rowCount === 0,
      existingStatus: row.status_code ?? null,
    };
  } catch (error) {
    if (error.code === '42P01') {
      state.logger?.warn?.('[billing] idempotency_keys table missing, skipping persistence.');
      return { skipBilling: false };
    }
    throw error;
  }
};

export const resolveTenant = async (req, res, next) => {
  try {
    const { redis, dbPool } = ensureConfigured();
    const billing = ensureReqState(req);

    let tenant = req.tenant ?? null;
    if (!tenant?.id) {
      let tenantId = req.user?.tenantId;

      if (!tenantId) {
        const apiKey = req.get('X-API-Key');
        if (!apiKey) {
          return sendError(res, req, 403, 'TENANT_MISSING', 'Tenant context is required.');
        }

        billing.apiKey = apiKey;
        tenantId = await resolveTenantIdFromApiKey(apiKey, { redis, dbPool });
        if (!tenantId) {
          return sendError(res, req, 401, 'AUTHENTICATION_REQUIRED', 'Authentication required.');
        }
      }

      tenant = await loadTenantById(tenantId, { redis, dbPool });
      if (!tenant) {
        return sendError(res, req, 403, 'TENANT_MISSING', 'Tenant context is required.');
      }
    }

    const plan = await loadPlan(tenant.planId, dbPool);
    billing.plan = plan;
    billing.quota = mergeQuota(plan, tenant.quotaOverride);
    billing.period = computePeriod(state.now());

    req.tenant = {
      id: tenant.id,
      name: tenant.name,
      planId: tenant.planId,
      plan: tenant.planId,
    };

    res.setHeader('X-Quota-Period', billing.period.key);
    if (billing.quota.limit != null) {
      res.setHeader('X-Quota-Limit', billing.quota.limit);
    }

    next();
  } catch (error) {
    req.log?.error?.({ err: error }, '[billing] resolveTenant failed');
    return sendError(res, req, 500, 'TENANT_RESOLUTION_FAILED', 'Tenant context could not be resolved.');
  }
};

export const startTimer = (req, _res, next) => {
  const billing = ensureReqState(req);
  billing.startedAt = state.now();
  billing.hrtime = typeof process?.hrtime?.bigint === 'function' ? process.hrtime.bigint() : null;
  next();
};

export const isWrite = (req) => !['GET', 'HEAD', 'OPTIONS'].includes((req.method || '').toUpperCase());

export const isBillable = (req) => isWrite(req) && getOperationContext(req).billable !== false;

const extractWeightOverride = (override) => {
  if (!override) return null;

  if (typeof override.weight === 'number') return override.weight;
  if (typeof override.call_weight === 'number') return override.call_weight;
  if (typeof override.operation_weight === 'number') return override.operation_weight;

  return null;
};

export const operationWeight = (req) => {
  const operation = getOperationContext(req);
  const override = ensureReqState(req).quota?.endpointOverrides?.[operation.normalizedEndpoint];
  const weight = extractWeightOverride(override) ?? operation.weight ?? 1;
  return weight > 0 ? weight : 1;
};

const extractEndpointLimit = (override) => {
  if (!override) return null;

  if (typeof override.limit === 'number') return override.limit;
  if (typeof override.monthly_api_calls === 'number') return override.monthly_api_calls;
  if (typeof override.monthlyLimit === 'number') return override.monthlyLimit;

  return null;
};

const coercePositiveNumber = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) {
    return null;
  }
  return num;
};

const extractReadRateLimitOverride = (override) => {
  if (!override) return null;

  const candidates = [
    override.read_rate_limit_per_minute,
    override.rate_limit_per_minute,
    override.per_minute,
    override.limit_per_minute,
    override.limit,
    override.rateLimitPerMinute,
  ];

  for (const candidate of candidates) {
    const value = coercePositiveNumber(candidate);
    if (value != null) {
      return value;
    }
  }

  return null;
};

const getReadRateLimitForTenant = (req) => {
  const billing = ensureReqState(req);
  const tenantPlanId = req.tenant?.plan ?? req.tenant?.planId ?? null;
  const overrides = billing.quota?.endpointOverrides ?? {};

  const overrideCandidates = [
    overrides.__read__,
    overrides.READ,
    overrides['GET'],
  ];

  for (const override of overrideCandidates) {
    const limit = extractReadRateLimitOverride(override);
    if (limit != null) {
      return limit;
    }
  }

  const plan = billing.plan ?? null;
  if (plan?.endpoint_overrides) {
    const planOverrideCandidates = [
      plan.endpoint_overrides.__read__,
      plan.endpoint_overrides.READ,
      plan.endpoint_overrides['GET'],
    ];

    for (const override of planOverrideCandidates) {
      const limit = extractReadRateLimitOverride(override);
      if (limit != null) {
        return limit;
      }
    }
  }

  const directCandidates = [
    billing.quota?.readRateLimitPerMinute,
    billing.quota?.rate_limit_per_minute,
    billing.quota?.rateLimitPerMinute,
    plan?.read_rate_limit_per_minute,
    plan?.rate_limit_per_minute,
    plan?.rateLimitPerMinute,
  ];

  for (const candidate of directCandidates) {
    const limit = coercePositiveNumber(candidate);
    if (limit != null) {
      return limit;
    }
  }

  if (tenantPlanId && state.readRateLimits instanceof Map) {
    const fallback = coercePositiveNumber(state.readRateLimits.get(tenantPlanId));
    if (fallback != null) {
      return fallback;
    }
  }

  return null;
};

export const incrementUsageAtomic = async (tenantId, endpoint, weight, periodKey, options = {}) => {
  if (!tenantId || !endpoint || !periodKey) {
    throw new Error('incrementUsageAtomic requires tenantId, endpoint, and periodKey.');
  }

  const { redis, dbPool } = ensureConfigured();
  const limit = Number.isFinite(options.limit) ? options.limit : null;
  const ttlSeconds = Number.isFinite(options.ttlSeconds) ? options.ttlSeconds : null;
  const key = `usage:${tenantId}:${periodKey}`;
  const ttlMs = ttlSeconds ? ttlSeconds * 1000 : null;

  if (redis) {
    try {
      const weightField = endpointWeightField(endpoint);
      const countField = endpointCountField(endpoint);
      const result = await redis.eval(
        REDIS_INCREMENT_LUA,
        1,
        key,
        ttlMs ?? 0,
        TOTAL_WEIGHT_FIELD,
        TOTAL_COUNT_FIELD,
        weightField,
        countField,
        weight,
        limit ?? -1,
      );

      const [allowedFlag, totalWeight, endpointWeight, totalCount, endpointCount] = Array.isArray(result)
        ? result
        : [0, 0, 0, 0, 0];

      return {
        allowed: allowedFlag === 1,
        total: Number(totalWeight ?? 0),
        endpointUsage: Number(endpointWeight ?? 0),
        totalCount: Number(totalCount ?? 0),
        endpointCount: Number(endpointCount ?? 0),
      };
    } catch (error) {
      state.logger?.warn?.({ err: error }, '[billing] Redis increment failed, falling back to Postgres.');
    }
  }

  const periodStart = options.periodStart ?? new Date(`${periodKey}-01T00:00:00.000Z`);
  const client = await dbPool.connect();

  try {
    await client.query('BEGIN');

    const current = await client.query(
      'SELECT count, total_weight FROM usage_counters WHERE tenant_id = $1 AND endpoint = $2 AND period_start = $3 FOR UPDATE',
      [tenantId, TOTAL_WEIGHT_FIELD, periodStart],
    );
    const currentTotalWeight = Number(current.rows[0]?.total_weight ?? 0);
    const currentTotalCount = Number(current.rows[0]?.count ?? 0);
    if (limit != null && currentTotalWeight + weight > limit) {
      await client.query('ROLLBACK');
      return {
        allowed: false,
        total: currentTotalWeight,
        endpointUsage: currentTotalWeight,
        totalCount: currentTotalCount,
        endpointCount: currentTotalCount,
      };
    }

    const total = await client.query(
      `INSERT INTO usage_counters (tenant_id, endpoint, period_start, count, total_weight)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (tenant_id, endpoint, period_start)
       DO UPDATE SET count = usage_counters.count + EXCLUDED.count,
                     total_weight = usage_counters.total_weight + EXCLUDED.total_weight
       RETURNING count, total_weight`,
      [tenantId, TOTAL_WEIGHT_FIELD, periodStart, 1, weight],
    );

    const endpointResult = await client.query(
      `INSERT INTO usage_counters (tenant_id, endpoint, period_start, count, total_weight)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (tenant_id, endpoint, period_start)
       DO UPDATE SET count = usage_counters.count + EXCLUDED.count,
                     total_weight = usage_counters.total_weight + EXCLUDED.total_weight
       RETURNING count, total_weight`,
      [tenantId, endpoint, periodStart, 1, weight],
    );

    await client.query('COMMIT');

    return {
      allowed: true,
      total: Number(total.rows[0]?.total_weight ?? 0),
      endpointUsage: Number(endpointResult.rows[0]?.total_weight ?? 0),
      totalCount: Number(total.rows[0]?.count ?? 0),
      endpointCount: Number(endpointResult.rows[0]?.count ?? 0),
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
};

const cleanupThresholdCache = () => {
  const now = Date.now();
  for (const [key, expiresAt] of state.thresholdCache.entries()) {
    if (expiresAt <= now) {
      state.thresholdCache.delete(key);
    }
  }
};

const markThresholdLogged = async (key, ttlSeconds) => {
  const ttl = Math.max(1, Math.floor(ttlSeconds));

  if (state.redis) {
    try {
      const result = await state.redis.set(key, '1', 'EX', ttl, 'NX');
      if (result === 'OK') {
        return true;
      }
      if (result) {
        return false;
      }
    } catch (error) {
      state.logger?.warn?.({ err: error }, '[billing] Failed to persist usage threshold marker to Redis.');
    }
  }

  cleanupThresholdCache();

  const now = Date.now();
  const expiresAt = now + ttl * 1000;
  const existing = state.thresholdCache.get(key) ?? 0;
  if (existing > now) {
    return false;
  }

  state.thresholdCache.set(key, expiresAt);
  return true;
};

const emitUsageThresholdEvents = async ({ tenantId, endpoint, usage, limit, period }) => {
  if (!tenantId || !endpoint) return;

  const limitValue = coercePositiveNumber(limit);
  if (limitValue == null) return;

  const usageValue = Number(usage);
  if (!Number.isFinite(usageValue) || usageValue < 0) return;

  const ratio = usageValue / limitValue;
  if (!Number.isFinite(ratio)) return;

  const ttlSeconds = Math.max(1, period?.ttlSeconds ?? 86_400);

  for (const threshold of USAGE_THRESHOLDS) {
    if (ratio >= threshold.value) {
      const key = `usage_threshold:${tenantId}:${endpoint}:${period?.key ?? 'unknown'}:${threshold.label}`;
      const shouldLog = await markThresholdLogged(key, ttlSeconds);
      if (shouldLog) {
        state.logger?.info?.(
          {
            event: 'usage.threshold',
            tenantId,
            endpoint,
            period: period?.key ?? null,
            threshold: threshold.value,
            usage: usageValue,
            limit: limitValue,
          },
          `Usage threshold ${threshold.label}% reached`,
        );
      }
    }
  }
};

export const enforceQuota = async (req, res, next) => {
  if (!isBillable(req)) return next();

  try {
    const billing = ensureReqState(req);
    const tenantId = req.tenant?.id;

    if (!tenantId) {
      return res.status(401).json({ code: 'TENANT_REQUIRED' });
    }

    const operation = getOperationContext(req);
    billing.period = billing.period ?? computePeriod(state.now());

    const idempotency = await upsertIdempotency(req, operation.normalizedEndpoint, tenantId);
    billing.idempotency = idempotency;
    if (idempotency.skipBilling) return next();

    const quotaLimit = billing.quota?.limit ?? null;
    const endpointLimit = extractEndpointLimit(billing.quota?.endpointOverrides?.[operation.normalizedEndpoint]);
    const limit = endpointLimit ?? quotaLimit;
    if (limit == null) return next();

    const weight = operationWeight(req);
    const usage = await incrementUsageAtomic(tenantId, operation.normalizedEndpoint, weight, billing.period.key, {
      limit,
      ttlSeconds: billing.period.ttlSeconds,
      periodStart: billing.period.start,
    });

    billing.usage = usage;
    billing.weight = weight;

    if (limit != null) {
      await emitUsageThresholdEvents({
        tenantId,
        endpoint: operation.normalizedEndpoint,
        usage: usage.total,
        limit,
        period: billing.period,
      });
    }

    if (!usage.allowed) {
      quotaBlockCounter.inc({
        tenant: typeof tenantId === 'string' && tenantId.trim().length > 0 ? tenantId : DEFAULT_TENANT_LABEL,
        endpoint: typeof operation.normalizedEndpoint === 'string' && operation.normalizedEndpoint.trim().length > 0
          ? operation.normalizedEndpoint
          : '/',
      });
      return res.status(429).json({
        code: 'QUOTA_EXCEEDED',
        remaining: Math.max(0, Math.floor(limit - usage.total)),
        resetAt: billing.period.end.toISOString(),
      });
    }

    res.setHeader('X-Quota-Remaining', Math.max(0, Math.floor(limit - usage.total)));

    next();
  } catch (error) {
    if (error.statusCode === 409) {
      return res.status(409).json({ code: 'IDEMPOTENCY_CONFLICT' });
    }

    req.log?.error?.({ err: error }, '[billing] enforceQuota failed');
    res.status(500).json({ code: 'BILLING_FAILURE' });
  }
};

const fallbackReadLimiter = (tenantId, nowMs, windowMs, limit) => {
  if (!tenantId) {
    return { allowed: true, count: 0, retryAfterMs: 0 };
  }

  const bucket = state.readLimiterCache.get(tenantId) ?? [];
  const cutoff = nowMs - windowMs;
  const filtered = bucket.filter((ts) => ts > cutoff);

  let allowed = true;
  let count;
  let retryAfterMs = 0;

  if (filtered.length >= limit) {
    allowed = false;
    count = filtered.length;
    retryAfterMs = Math.max(0, (filtered[0] + windowMs) - nowMs);
  } else {
    filtered.push(nowMs);
    count = filtered.length;
    if (count >= limit) {
      const firstTs = filtered[0] ?? nowMs;
      retryAfterMs = Math.max(0, (firstTs + windowMs) - nowMs);
    }
  }

  state.readLimiterCache.set(tenantId, filtered);
  return { allowed, count, retryAfterMs };
};

export const enforceReadRateLimit = async (req, res, next) => {
  if ((req.method || '').toUpperCase() !== 'GET') {
    return next();
  }

  try {
    const { redis } = ensureConfigured();
    const tenantId = req.tenant?.id;
    if (!tenantId) {
      return next();
    }

    const limit = getReadRateLimitForTenant(req);
    const limitValueRaw = coercePositiveNumber(limit);
    if (limitValueRaw == null) {
      return next();
    }

    const limitValue = Math.max(1, Math.floor(limitValueRaw));

    const windowMs = state.readRateLimitWindowMs ?? READ_RATE_LIMIT_WINDOW_MS_DEFAULT;
    const nowMs = Date.now();
    const ttlSeconds = Math.max(1, Math.ceil(windowMs / 1000) * 2);

    let result = { allowed: true, count: 0, retryAfterMs: 0 };

    if (redis) {
      try {
        const evalResult = await redis.eval(
          READ_RATE_LIMIT_LUA,
          2,
          `read_rate:${tenantId}`,
          `read_rate:${tenantId}:seq`,
          nowMs,
          windowMs,
          limitValue,
          ttlSeconds,
        );

        result = {
          allowed: evalResult?.[0] === 1,
          count: Number(evalResult?.[1] ?? 0),
          retryAfterMs: Number(evalResult?.[2] ?? 0),
        };
      } catch (error) {
        state.logger?.warn?.({ err: error }, '[billing] GET rate limiter Redis failure, using in-memory window.');
        result = fallbackReadLimiter(tenantId, nowMs, windowMs, limitValue);
      }
    } else {
      result = fallbackReadLimiter(tenantId, nowMs, windowMs, limitValue);
    }

    result.count = Number.isFinite(result.count) ? result.count : 0;
    result.retryAfterMs = Number.isFinite(result.retryAfterMs) ? Math.max(0, result.retryAfterMs) : 0;

    const remaining = Math.max(0, Math.floor(limitValue - result.count));
    res.setHeader('X-RateLimit-Limit', limitValue);
    res.setHeader('X-RateLimit-Remaining', remaining);

    const retryAfterSeconds = Math.ceil((result.retryAfterMs || 0) / 1000);
    if (retryAfterSeconds > 0) {
      res.setHeader('Retry-After', retryAfterSeconds);
      const resetEpoch = Math.ceil((nowMs + result.retryAfterMs) / 1000);
      res.setHeader('X-RateLimit-Reset', resetEpoch);
    } else {
      const resetEpoch = Math.ceil((nowMs + windowMs) / 1000);
      res.setHeader('X-RateLimit-Reset', resetEpoch);
    }

    const billing = ensureReqState(req);
    billing.readRateLimit = limitValue;
    billing.readRateRemaining = remaining;

    if (!result.allowed) {
      const operation = getOperationContext(req);
      req.log?.warn?.({ tenantId, endpoint: operation.normalizedEndpoint, limit: limitValue, count: result.count }, '[billing] GET rate limit exceeded');
      return sendError(res, req, 429, 'READ_RATE_LIMIT_EXCEEDED', 'Too many read requests. Please slow down.');
    }

    return next();
  } catch (error) {
    req.log?.error?.({ err: error }, '[billing] Read rate limiter failure');
    return sendError(res, req, 500, 'READ_RATE_LIMIT_FAILURE', 'The read rate limiter is temporarily unavailable.');
  }
};

export const finalizeAndLog = (req, res, next) => {
  const billing = ensureReqState(req);
  const startTime = billing.hrtime;
  const startedAt = billing.startedAt ?? state.now();
  const operation = getOperationContext(req);

  res.once('finish', async () => {
    try {
      const { dbPool } = ensureConfigured();
      const end = typeof process?.hrtime?.bigint === 'function' ? process.hrtime.bigint() : null;
      const durationMs = startTime && end ? Number(end - startTime) / 1e6 : state.now() - startedAt;
      const method = (req.method || '').toUpperCase();
      const status = res.statusCode;
      const endpoint = operation.normalizedEndpoint;
      const billable = isBillable(req);

      requestCounter.labels(method, endpoint, String(status), billable ? 'true' : 'false').inc();
      durationMetric.observe({ method, endpoint, status: String(status) }, durationMs);

      const tenantId = req.tenant?.id ?? null;
      if (tenantId) {
        const requestId = ensureRequestId(req, res);
        try {
          await dbPool.query(
            `INSERT INTO api_events (tenant_id, endpoint, event_type, status_code, request_id, metadata)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [
              tenantId,
              endpoint,
              method,
              status,
              requestId,
              {
                duration_ms: durationMs,
                billable,
                weight: billing.weight ?? operationWeight(req),
                usage: billing.usage ?? null,
                idempotency: billing.idempotency?.key ?? null,
              },
            ],
          );
        } catch (insertError) {
          analyticsInsertFailuresTotal.inc();
          throw insertError;
        }
      }

      if (billing.idempotency?.key) {
        try {
          await dbPool.query(
            'UPDATE idempotency_keys SET status_code = $2, locked_at = NULL, last_accessed_at = NOW() WHERE idempotency_key = $1',
            [billing.idempotency.key, status],
          );
        } catch (error) {
          if (error.code !== '42P01') {
            state.logger?.warn?.({ err: error }, '[billing] Failed to update idempotency status.');
          }
        }
      }
    } catch (error) {
      req.log?.error?.({ err: error }, '[billing] finalizeAndLog failure');
    }
  });

  if (typeof next === 'function') {
    next();
  }
};

export const createBillingMiddleware = ({
  redis,
  dbPool,
  now,
  idempotencyTtlSeconds,
  logger,
  readRateLimits,
  readRateLimitWindowMs,
} = {}) => {
  configureBilling({ redis, dbPool, now, idempotencyTtlSeconds, logger, readRateLimits, readRateLimitWindowMs });
  const chain = [resolveTenant, startTimer, enforceQuota, finalizeAndLog];
  chain.rateLimitRead = enforceReadRateLimit;
  return chain;
};

export default {
  configureBilling,
  resolveTenant,
  startTimer,
  isWrite,
  isBillable,
  operationWeight,
  incrementUsageAtomic,
  enforceQuota,
  finalizeAndLog,
  createBillingMiddleware,
};
