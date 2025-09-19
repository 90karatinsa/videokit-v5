import jwt from 'jsonwebtoken';

import { sendError } from './http-error.js';
import { fetchTenantRow, normalizeTenantRow } from '../tenant-schema.js';

export const SESSION_COOKIE_NAME = 'videokit_session';

/**
 * Creates authentication helpers (protect/authorize) backed by JWT sessions.
 * @param {{ dbPool: import('pg').Pool, config: import('../config.js').default }} params
 */
export function createAuthMiddleware({ dbPool, config }) {
  if (!dbPool) {
    throw new Error('createAuthMiddleware requires a database pool');
  }
  const jwtSecret = config?.secrets?.jwtSecret;
  if (!jwtSecret) {
    throw new Error('JWT secret is not configured.');
  }

  const cookieBaseOptions = {
    httpOnly: true,
    sameSite: 'lax',
    path: '/',
    maxAge: 30 * 24 * 60 * 60 * 1000, // 30 days
  };

  const resolveCookieSecure = (req) => {
    const forced = process.env.COOKIE_SECURE?.trim()?.toLowerCase();
    if (forced === 'true') {
      return true;
    }
    if (forced === 'false') {
      return false;
    }

    const isHttps = () => {
      if (!req) {
        return process.env.NODE_ENV === 'production';
      }
      if (req.secure) {
        return true;
      }
      const forwardedProto = req.get?.('X-Forwarded-Proto');
      if (forwardedProto) {
        const [primary] = forwardedProto.split(',');
        if (primary?.trim()?.toLowerCase() === 'https') {
          return true;
        }
      }
      return false;
    };

    return isHttps();
  };

  const buildCookieOptions = (req) => ({
    ...cookieBaseOptions,
    secure: resolveCookieSecure(req),
  });

  const normalizeRoles = (roles = []) => roles.map((role) => role?.toLowerCase?.() ?? role).filter(Boolean);
  
  // --- DEĞİŞİKLİK BURADA BAŞLIYOR ---

  const loadUserContext = async (userId) => {
    // 1. Sorgu güncellendi: Artık var olmayan 'full_name' istenmiyor.
    const userResult = await dbPool.query(
      'SELECT id, email, role, tenant_id, full_name, created_at, updated_at FROM users WHERE id = $1',
      [userId],
    );

    if (userResult.rowCount === 0) {
      return null;
    }
    const userRow = userResult.rows[0];
    const { tenant_id: tenantId, role } = userRow;

    let tenant = null;
    if (tenantId) {
      const tenantRow = await fetchTenantRow(dbPool, tenantId, {
        includeName: true,
        includePlan: true,
        includeQuota: true,
        includeTimestamps: true,
      });
      tenant = normalizeTenantRow(tenantRow) ?? null;
    }
    
    // 2. Karmaşık rol çekme mantığı kaldırıldı. Rol doğrudan user objesinden alınıyor.
    // Artık 'user_tenant_roles' tablosuna ihtiyaç yok.
    const roles = role ? [role] : [];

    return {
      user: {
        id: userRow.id,
        email: userRow.email,
        fullName: userRow.full_name, // Bu hala undefined dönebilir ama çökertmez.
        role: userRow.role,
        tenantId: tenant?.id ?? tenantId,
        createdAt: userRow.created_at,
        updatedAt: userRow.updated_at,
        roles,
      },
      tenant,
    };
  };
  
  // --- DEĞİŞİKLİK BURADA BİTİYOR ---

  const clearSessionCookie = (req, res) => {
    const options = buildCookieOptions(req);
    res.clearCookie(SESSION_COOKIE_NAME, { ...options, maxAge: 0 });
  };

  const extractToken = (req) => {
    const cookieToken = req.cookies?.[SESSION_COOKIE_NAME];
    if (cookieToken) return cookieToken;

    const authHeader = req.get('Authorization');
    if (authHeader?.startsWith('Bearer ')) {
      return authHeader.slice(7).trim();
    }

    return null;
  };

  const protect = async (req, res, next) => {
    const token = extractToken(req);

    if (token) {
      try {
        const payload = jwt.verify(token, jwtSecret);
        // payload.tenantId'ye artık ihtiyacımız yok, bilgiyi direkt user'dan alıyoruz.
        const context = await loadUserContext(payload.sub);
        if (!context) {
          clearSessionCookie(req, res);
          return sendError(res, req, 401, 'AUTHENTICATION_REQUIRED', 'Authentication required.');
        }

        req.user = context.user;
        req.tenant = context.tenant ?? req.tenant;
        req.authType = 'session';
        return next();
      } catch (error) {
        req.log?.warn?.({ err: error }, '[auth] Invalid session token.');
        clearSessionCookie(req, res);
        return sendError(res, req, 401, 'AUTHENTICATION_REQUIRED', 'Authentication required.');
      }
    }

    const apiKey = req.get('X-API-Key');
    if (apiKey) {
      req.authType = 'apiKey';
      return next();
    }

    return sendError(res, req, 401, 'AUTHENTICATION_REQUIRED', 'Authentication required.');
  };

  const authorize = (...requiredRoles) => {
    const normalizedRequired = normalizeRoles(requiredRoles);
    return (req, res, next) => {
      if (!req.user) {
        return sendError(res, req, 401, 'AUTHENTICATION_REQUIRED', 'Authentication required.');
      }

      if (normalizedRequired.length === 0) {
        return next();
      }

      const userRoles = new Set(normalizeRoles(req.user.roles));
      if (userRoles.has('superadmin')) {
        return next();
      }

      const hasRole = normalizedRequired.some((role) => userRoles.has(role));
      if (!hasRole) {
        return sendError(res, req, 403, 'FORBIDDEN_ROLE', 'Forbidden: insufficient role.');
      }

      return next();
    };
  };

  const issueSession = (req, res, { userId, tenantId, roles }) => {
    const token = jwt.sign(
      {
        sub: userId,
        tenantId: tenantId ?? null,
        roles: normalizeRoles(roles),
      },
      jwtSecret,
      { expiresIn: config?.jwt?.expiresIn ?? '30d' },
    );

    const options = buildCookieOptions(req);
    res.cookie(SESSION_COOKIE_NAME, token, options);
    return token;
  };

  const destroySession = (req, res) => {
    clearSessionCookie(req, res);
  };

  return {
    protect,
    authorize,
    issueSession,
    destroySession,
    loadUserContext,
    sessionCookieName: SESSION_COOKIE_NAME,
    cookieOptions: cookieBaseOptions,
  };
}

export default createAuthMiddleware;
