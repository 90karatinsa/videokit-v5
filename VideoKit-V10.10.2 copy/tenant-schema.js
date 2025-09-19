let tenantMetadataCache = null;

function normalizeColumnName(name) {
  if (!name) return null;
  return String(name).trim().toLowerCase();
}

function buildSelectClause(metadata, options = {}) {
  const {
    includeName = true,
    includePlan = true,
    includeQuota = true,
    includeTimestamps = true,
    additionalColumns = [],
  } = options;

  const selectClauses = [];
  const seen = new Set();
  const push = (expression) => {
    if (!expression) return;
    if (seen.has(expression)) return;
    selectClauses.push(expression);
    seen.add(expression);
  };

  const { columns, idColumn } = metadata;
  if (!idColumn) {
    throw new Error('Tenants table must contain an "id" or "tenant_id" column.');
  }

  push(`${idColumn} AS id`);

  if (idColumn !== 'tenant_id' && columns.has('tenant_id')) {
    push('tenant_id');
  }
  if (idColumn !== 'id' && columns.has('id')) {
    push('id');
  }

  if (includeName && columns.has('name')) {
    push('name');
  }

  if (includePlan) {
    if (columns.has('plan_id')) {
      push('plan_id');
    }
    if (columns.has('plan')) {
      push('plan');
    }
  }

  if (includeQuota && columns.has('quota_override')) {
    push('quota_override');
  }

  if (includeTimestamps) {
    if (columns.has('created_at')) {
      push('created_at');
    }
    if (columns.has('updated_at')) {
      push('updated_at');
    }
  }

  for (const column of additionalColumns) {
    const normalized = normalizeColumnName(column);
    if (!normalized) continue;
    if (columns.has(normalized)) {
      push(normalized);
    }
  }

  return selectClauses;
}

export function clearTenantMetadataCache() {
  tenantMetadataCache = null;
}

export async function getTenantMetadata(dbPool) {
  if (tenantMetadataCache) {
    return tenantMetadataCache;
  }

  if (!dbPool) {
    throw new Error('Database pool is required to inspect tenant metadata.');
  }

  const result = await dbPool.query(
    'SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2',
    ['public', 'tenants'],
  );

  const columns = new Set(result.rows.map((row) => normalizeColumnName(row.column_name)));
  const idColumn = columns.has('tenant_id') ? 'tenant_id' : (columns.has('id') ? 'id' : null);

  tenantMetadataCache = { columns, idColumn };
  return tenantMetadataCache;
}

async function fetchTenantRowWithMetadata(dbPool, tenantId, metadata, options) {
  if (!tenantId) {
    return null;
  }

  const selectClauses = buildSelectClause(metadata, options);
  if (selectClauses.length === 0) {
    throw new Error('Tenants table does not expose any selectable columns.');
  }

  const sql = `SELECT ${selectClauses.join(', ')} FROM tenants WHERE ${metadata.idColumn} = $1`;
  const result = await dbPool.query(sql, [tenantId]);
  return result.rows[0] ?? null;
}

export async function fetchTenantRow(dbPool, tenantId, options = {}) {
  try {
    const metadata = await getTenantMetadata(dbPool);
    if (!metadata.idColumn) {
      throw new Error('Tenants table must contain an "id" or "tenant_id" column.');
    }
    return await fetchTenantRowWithMetadata(dbPool, tenantId, metadata, options);
  } catch (error) {
    if ((error?.code === '42703' || error?.code === '42P01') && !options.__retry) {
      clearTenantMetadataCache();
      return fetchTenantRow(dbPool, tenantId, { ...options, __retry: true });
    }
    throw error;
  }
}

export function normalizeTenantRow(row, { fallbackPlanId = null } = {}) {
  if (!row) {
    return null;
  }

  const normalized = { ...row };

  if (normalized.tenantid && normalized.tenant_id == null) {
    normalized.tenant_id = normalized.tenantid;
  }
  if (normalized.id == null && normalized.tenant_id != null) {
    normalized.id = normalized.tenant_id;
  }
  if (normalized.tenant_id == null && normalized.id != null) {
    normalized.tenant_id = normalized.id;
  }

  if (normalized.createdat != null && normalized.created_at == null) {
    normalized.created_at = normalized.createdat;
  }
  if (normalized.updatedat != null && normalized.updated_at == null) {
    normalized.updated_at = normalized.updatedat;
  }

  const resolvedPlan = normalized.plan_id ?? normalized.plan ?? fallbackPlanId;
  if (resolvedPlan != null) {
    if (normalized.plan_id == null) {
      normalized.plan_id = resolvedPlan;
    }
    if (normalized.plan == null) {
      normalized.plan = resolvedPlan;
    }
  }

  return normalized;
}
