import express from 'express';
import dotenv from 'dotenv';
import cron from 'node-cron';
import Database from 'better-sqlite3';
import fetch from 'node-fetch';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

dotenv.config();
const app = express();
app.use(express.json({ limit: '3mb' }));

const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || 'http://localhost:3000').replace(/\/$/, '');
const CRON_TIME = process.env.CRON_TIME || '0 9 * * *';
const CRON_TZ = process.env.CRON_TZ || null;
const ANALYTICS_SALT = process.env.ANALYTICS_SALT || '';
let cronTzFallbackWarned = false;
const DB_PATH = process.env.DB_PATH || 'db.sqlite';
const PORT = Number(process.env.PORT || 3000);
const RUNTIME_DISABLED = process.env.DISABLE_RUNTIME === '1' || process.env.NODE_ENV === 'test';
const ENABLE_SECURITY_HEADERS = process.env.ENABLE_SECURITY_HEADERS !== '0';
const CORS_ORIGIN_RAW = normalizeText(process.env.CORS_ORIGIN) || '*';
const CORS_ALLOWED_ORIGINS = CORS_ORIGIN_RAW === '*'
 ? ['*']
 : CORS_ORIGIN_RAW.split(',').map((item) => item.trim()).filter(Boolean);

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');

const uploadsDir = path.join(process.cwd(), 'uploads');
if (!fs.existsSync(uploadsDir)) {
 fs.mkdirSync(uploadsDir, { recursive: true });
}

const allowedPlatforms = new Set(['telegram']);
const MAX_POST_BUTTONS = 8;
const MAX_POST_BUTTON_TEXT_LENGTH = 64;
const DEFAULT_BROADCAST_INTERVAL_MINUTES = parsePositiveInt(process.env.MIN_INTERVAL_MINUTES, 5, 1, 1440);
const DEFAULT_LOGS_PAGE_SIZE = 50;
const MAX_LOGS_PAGE_SIZE = 200;
const ANALYTICS_CACHE_TTL_MS = parsePositiveInt(process.env.ANALYTICS_CACHE_TTL_MS, 30000, 1000, 3600000);
const FORECAST_CACHE_TTL_MS = parsePositiveInt(process.env.FORECAST_CACHE_TTL_MS, 20000, 1000, 600000);
const FORECAST_CACHE_MAX_ITEMS = parsePositiveInt(process.env.FORECAST_CACHE_MAX_ITEMS, 120, 10, 1000);
const TELEGRAM_API_TIMEOUT_MS = parsePositiveInt(process.env.TELEGRAM_API_TIMEOUT_MS, 15000, 1000, 120000);
const TELEGRAM_API_MAX_RETRIES = parsePositiveInt(process.env.TELEGRAM_API_MAX_RETRIES, 2, 0, 6);
const TELEGRAM_API_RETRY_BASE_MS = parsePositiveInt(process.env.TELEGRAM_API_RETRY_BASE_MS, 1000, 100, 30000);
const TELEGRAM_API_RETRY_MAX_MS = parsePositiveInt(process.env.TELEGRAM_API_RETRY_MAX_MS, 15000, 1000, 120000);

app.use((req, res, next) => {
 const requestOrigin = normalizeText(req.headers.origin);
 if (CORS_ALLOWED_ORIGINS.includes('*')) {
  res.setHeader('Access-Control-Allow-Origin', '*');
 } else if (requestOrigin && CORS_ALLOWED_ORIGINS.includes(requestOrigin)) {
  res.setHeader('Access-Control-Allow-Origin', requestOrigin);
  res.setHeader('Vary', 'Origin');
 }
 res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
 res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
 if (ENABLE_SECURITY_HEADERS) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'same-origin');
  res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');
  const forwardedProto = normalizeText(req.headers['x-forwarded-proto']);
  if (req.secure || forwardedProto === 'https') {
   res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  }
 }
 if (req.method === 'OPTIONS') return res.sendStatus(200);
 next();
});

function badRequest(res, message) {
 return res.status(400).send({ ok: false, error: message });
}

function isValidDateString(value) {
 return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function addDaysToDateString(value, days) {
 if (!isValidDateString(value)) return null;
 const [yy, mm, dd] = value.split('-').map(Number);
 const date = new Date(yy, mm - 1, dd);
 if (Number.isNaN(date.getTime())) return null;
 date.setDate(date.getDate() + days);
 const pad = (num) => String(num).padStart(2, '0');
 return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function maxDateString(...values) {
 const valid = values.filter(isValidDateString);
 if (!valid.length) return null;
 return valid.sort().slice(-1)[0];
}

function ensureColumn(table, column, definition) {
 const columns = db.prepare(`PRAGMA table_info(${table})`).all();
 if (!columns.find(col => col.name === column)) {
  db.prepare(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`).run();
 }
}

function getSetting(key) {
 const row = db.prepare('SELECT value FROM settings WHERE key = ?').get(key);
 return row ? row.value : null;
}

function setSetting(key, value) {
 db.prepare('INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
  .run(key, value);
}

function normalizeText(value) {
 const trimmed = String(value || '').trim();
 return trimmed ? trimmed : null;
}

function normalizeUrl(value) {
 const trimmed = String(value || '').trim();
 return trimmed ? trimmed : null;
}

function parsePositiveInt(value, fallback, min = 1, max = Number.MAX_SAFE_INTEGER) {
 const parsed = Number(value);
 if (!Number.isFinite(parsed)) return fallback;
 const normalized = Math.floor(parsed);
 if (normalized < min) return fallback;
 return Math.min(normalized, max);
}

function normalizeLogStatus(value) {
 const normalized = String(value || '').trim().toLowerCase();
 if (normalized === 'sent' || normalized === 'failed' || normalized === 'pending') {
  return normalized;
 }
 return '';
}

function escapeSqlLikePattern(value) {
 return String(value || '')
  .replace(/\\/g, '\\\\')
  .replace(/%/g, '\\%')
  .replace(/_/g, '\\_');
}

function buildLogsQueryParts(filters = {}) {
 const status = normalizeLogStatus(filters.status);
 const queryText = normalizeText(filters.query);
 const clauses = [`logs.platform = 'telegram'`];
 const params = [];

 if (status) {
  clauses.push('logs.status = ?');
  params.push(status);
 }

 if (queryText) {
  const like = `%${escapeSqlLikePattern(String(queryText).toLowerCase())}%`;
  const searchableFields = [
   'LOWER(CAST(logs.id AS TEXT))',
   'LOWER(CAST(logs.postId AS TEXT))',
   'LOWER(CAST(COALESCE(logs.companyId, posts.companyId) AS TEXT))',
   'LOWER(COALESCE(logs.companyName, companies.name, \'\'))',
   'LOWER(COALESCE(logs.error, \'\'))',
   'LOWER(COALESCE(posts.text, \'\'))',
   'LOWER(COALESCE(drafts.caption, \'\'))',
   'LOWER(COALESCE(drafts.text, \'\'))',
   'LOWER(COALESCE(logs.trigger, \'\'))',
   'LOWER(COALESCE(logs.status, \'\'))',
   'LOWER(COALESCE(logs.createdAt, logs.date, \'\'))'
  ];
  clauses.push(`(${searchableFields.map((field) => `${field} LIKE ? ESCAPE '\\'`).join(' OR ')})`);
  for (let i = 0; i < searchableFields.length; i += 1) {
   params.push(like);
  }
 }

 return {
  whereSql: `WHERE ${clauses.join('\n  AND ')}`,
  params,
  status,
  queryText: queryText || ''
 };
}

function isHttpUrl(value) {
 if (!value) return false;
 try {
  const url = new URL(value);
  return url.protocol === 'http:' || url.protocol === 'https:';
 } catch (e) {
  return false;
 }
}

function parseStoredButtons(value) {
 if (value === undefined || value === null) return [];
 let parsed = value;
 if (typeof value === 'string') {
  const trimmed = value.trim();
  if (!trimmed) return [];
  try {
   parsed = JSON.parse(trimmed);
  } catch (e) {
   return [];
  }
 }
 if (!Array.isArray(parsed)) return [];
 return parsed
  .map((item) => {
   if (!item || typeof item !== 'object') return null;
   const text = normalizeText(item.text);
   const url = normalizeUrl(item.url);
   if (!text || !url || !isHttpUrl(url)) return null;
   return {
    text: text.slice(0, MAX_POST_BUTTON_TEXT_LENGTH),
    url
   };
  })
  .filter(Boolean)
  .slice(0, MAX_POST_BUTTONS);
}

function normalizePostButtonsInput(value) {
 if (value === undefined || value === null || value === '') return [];
 let parsed = value;
 if (typeof parsed === 'string') {
  try {
   parsed = JSON.parse(parsed);
  } catch (e) {
   throw new Error('Buttons payload is invalid');
  }
 }
 if (!Array.isArray(parsed)) {
  throw new Error('Buttons must be an array');
 }
 if (parsed.length > MAX_POST_BUTTONS) {
  throw new Error(`Maximum ${MAX_POST_BUTTONS} buttons allowed`);
 }
 return parsed.map((item, index) => {
  if (!item || typeof item !== 'object') {
   throw new Error(`Button #${index + 1} is invalid`);
  }
  const text = normalizeText(item.text);
  const url = normalizeUrl(item.url);
  if (!text || !url) {
   throw new Error(`Button #${index + 1} requires text and url`);
  }
  if (!isHttpUrl(url)) {
   throw new Error(`Button #${index + 1} has invalid url`);
  }
  return {
   text: text.slice(0, MAX_POST_BUTTON_TEXT_LENGTH),
   url
  };
 });
}

function toHttpUrl(value) {
 const normalized = normalizeUrl(value);
 if (!normalized) return null;
 if (isHttpUrl(normalized)) return normalized;
 if (/^www\./i.test(normalized) || /^[^\s]+\.[^\s]+/.test(normalized)) {
  const withScheme = `https://${normalized.replace(/^\/+/, '')}`;
  if (isHttpUrl(withScheme)) return withScheme;
 }
 return null;
}

function extractButtonsFromReplyMarkup(replyMarkup) {
 if (!replyMarkup || !Array.isArray(replyMarkup.inline_keyboard)) return [];
 const buttons = [];
 for (const row of replyMarkup.inline_keyboard) {
  if (!Array.isArray(row)) continue;
  for (const item of row) {
   if (!item || typeof item !== 'object') continue;
   const text = normalizeText(item.text);
   const url = toHttpUrl(item.url);
   if (!text || !url) continue;
   buttons.push({
    text: text.slice(0, MAX_POST_BUTTON_TEXT_LENGTH),
    url
   });
   if (buttons.length >= MAX_POST_BUTTONS) return buttons;
  }
 }
 return buttons;
}

function extractButtonsFromEntities(sourceText, entities) {
 const source = String(sourceText || '');
 if (!source || !Array.isArray(entities) || !entities.length) return [];
 const buttons = [];
 for (const entity of entities) {
  if (!entity || typeof entity !== 'object') continue;
  const offset = Number(entity.offset);
  const length = Number(entity.length);
  if (!Number.isInteger(offset) || !Number.isInteger(length) || offset < 0 || length <= 0) continue;
  const label = normalizeText(source.slice(offset, offset + length));
  let rawUrl = null;
  if (entity.type === 'text_link') {
   rawUrl = entity.url;
  } else if (entity.type === 'url') {
   rawUrl = source.slice(offset, offset + length);
  } else {
   continue;
  }
  const url = toHttpUrl(rawUrl);
  if (!url) continue;
  buttons.push({
   text: (label || url).slice(0, MAX_POST_BUTTON_TEXT_LENGTH),
   url
  });
  if (buttons.length >= MAX_POST_BUTTONS) return buttons;
 }
 return buttons;
}

function getCtaUrl(post) {
 return normalizeUrl(post.ctaUrl) || null;
}

function formatCtaLabel(ctaUrl, ctaLabel) {
 const label = normalizeText(ctaLabel);
 if (label) return label.slice(0, 28);
 if (!ctaUrl) return 'Telegram';
 try {
  const url = new URL(ctaUrl);
  const host = url.hostname.replace('www.', '');
  const pathPart = url.pathname && url.pathname !== '/' ? url.pathname.replace(/^\//, '').split('/')[0] : '';
  const derived = pathPart ? `${host}/${pathPart}` : host;
  return derived.slice(0, 28);
 } catch (e) {
  return 'Telegram';
 }
}

function buildTrackedUrl(code) {
 if (!code) return null;
 if (!PUBLIC_BASE_URL) return null;
 return `${PUBLIC_BASE_URL}/r/${code}`;
}

function resolveCtaUrl(post) {
 return buildTrackedUrl(post.linkCode) || getCtaUrl(post);
}

function buildTelegramReplyMarkup(post) {
 const buttons = parseStoredButtons(post.buttons);
 if (buttons.length) {
  return {
   inline_keyboard: buttons.map((button) => ([{
    text: button.text,
    url: button.url
   }]))
  };
 }
 const ctaUrl = resolveCtaUrl(post);
 if (!ctaUrl) return null;
 const ctaLabel = formatCtaLabel(ctaUrl, post.ctaLabel);
 return { inline_keyboard: [[{ text: ctaLabel, url: ctaUrl }]] };
}

function generateCode(length = 7) {
 const raw = crypto.randomBytes(length + 4).toString('base64').replace(/[+/=]/g, '');
 return raw.slice(0, length);
}

function hashIp(ip) {
 if (!ip) return null;
 return crypto.createHash('sha256').update(`${ip}${ANALYTICS_SALT}`).digest('hex');
}

function createLink(url, postId) {
 const createdAt = new Date().toISOString();
 for (let attempt = 0; attempt < 6; attempt += 1) {
  const code = generateCode(7);
  try {
   const result = db.prepare(`INSERT INTO links (code,url,postId,createdAt,clickCount) VALUES (?,?,?,?,0)`)
    .run(code, url, postId, createdAt);
   return { id: result.lastInsertRowid, code };
  } catch (e) {
   if (String(e.message).toLowerCase().includes('unique')) continue;
   throw e;
  }
 }
 throw new Error('Failed to create short link');
}

function ensurePostLink(postId, ctaUrl, existingLinkId, trackLinks) {
 if (!trackLinks) return null;
 const normalizedUrl = normalizeUrl(ctaUrl);
 if (!normalizedUrl) return null;
 if (existingLinkId) {
  const existing = db.prepare('SELECT id,url FROM links WHERE id = ?').get(existingLinkId);
  if (existing && existing.url === normalizedUrl) return existing.id;
 }
 const link = createLink(normalizedUrl, postId);
 return link.id;
}

function normalizeTime(value) {
 if (!value) return null;
 const trimmed = String(value).trim();
 if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(trimmed)) return null;
 const [h, m] = trimmed.split(':').map(Number);
 return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
}

function normalizePremiumFlag(value) {
 return value === 1 || value === true || value === '1' ? 1 : 0;
}

function timeToMinutes(value) {
 const normalized = normalizeTime(value);
 if (!normalized) return null;
 const [h, m] = normalized.split(':').map(Number);
 return h * 60 + m;
}

function warnCronTzFallback(reason) {
 if (!CRON_TZ || cronTzFallbackWarned) return;
 cronTzFallbackWarned = true;
 const reasonText = reason ? ` (${reason})` : '';
 console.warn(`CRON_TZ fallback to server local time${reasonText}. CRON_TZ="${CRON_TZ}"`);
}

function resolveNowParts() {
 const fallback = new Date();
 const fallbackParts = {
  year: fallback.getFullYear(),
  month: fallback.getMonth() + 1,
  day: fallback.getDate(),
  hour: fallback.getHours(),
  minute: fallback.getMinutes(),
  second: fallback.getSeconds()
 };
 if (!CRON_TZ) return fallbackParts;
 try {
  const formatter = new Intl.DateTimeFormat('en-US', {
   timeZone: CRON_TZ,
   year: 'numeric',
   month: '2-digit',
   day: '2-digit',
   hour: '2-digit',
   minute: '2-digit',
   second: '2-digit',
   hourCycle: 'h23'
  });
  const parts = {};
  for (const part of formatter.formatToParts(new Date())) {
   if (part.type === 'literal') continue;
   parts[part.type] = Number(part.value);
  }
  if (
   Number.isFinite(parts.year) &&
   Number.isFinite(parts.month) &&
   Number.isFinite(parts.day) &&
   Number.isFinite(parts.hour) &&
   Number.isFinite(parts.minute) &&
   Number.isFinite(parts.second)
  ) {
   return {
    year: parts.year,
    month: parts.month,
    day: parts.day,
    hour: parts.hour,
    minute: parts.minute,
    second: parts.second
   };
  }
  warnCronTzFallback('invalid time zone parts');
 } catch (e) {
  warnCronTzFallback(e?.message || 'invalid timezone');
 }
 return fallbackParts;
}

function getCurrentDateString() {
 const now = resolveNowParts();
 const pad = (num) => String(num).padStart(2, '0');
 return `${now.year}-${pad(now.month)}-${pad(now.day)}`;
}

function getCurrentMinutes() {
 const now = resolveNowParts();
 return now.hour * 60 + now.minute;
}

function minutesToIso(date, minutes) {
 if (!isValidDateString(date) || !Number.isFinite(minutes)) return null;
 const [yy, mm, dd] = date.split('-').map(Number);
 const baseUtc = Date.UTC(yy, mm - 1, dd, 0, 0, 0);
 const shifted = new Date(baseUtc + (Math.floor(minutes) * 60 * 1000));
 const pad = (num) => String(num).padStart(2, '0');
 return `${shifted.getUTCFullYear()}-${pad(shifted.getUTCMonth() + 1)}-${pad(shifted.getUTCDate())}T${pad(shifted.getUTCHours())}:${pad(shifted.getUTCMinutes())}:00`;
}

function nowLocalIso() {
 const now = resolveNowParts();
 const pad = (num) => String(num).padStart(2, '0');
 return `${now.year}-${pad(now.month)}-${pad(now.day)}T${pad(now.hour)}:${pad(now.minute)}:${pad(now.second)}`;
}

function getPrimarySchedulerDefault() {
 const cronMatch = String(CRON_TIME || '').match(/^(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\*$/);
 if (cronMatch) {
  const mm = String(cronMatch[1]).padStart(2, '0');
  const hh = String(cronMatch[2]).padStart(2, '0');
  return `${hh}:${mm}`;
 }
 return '09:00';
}

function getSecondarySchedulerDefault(primaryStartTime) {
 const envValue = normalizeTime(process.env.SECOND_SCHEDULE_TIME);
 if (!envValue) return null;
 if (envValue === primaryStartTime) return null;
 return envValue;
}

function getSchedulerSettings() {
 const scheduleTime = normalizeTime(getSetting('scheduleTime')) || getPrimarySchedulerDefault();
 const rawSecondFromSetting = getSetting('secondScheduleTime');
 let secondScheduleTime = null;
 if (rawSecondFromSetting === null) {
  secondScheduleTime = getSecondarySchedulerDefault(scheduleTime);
 } else if (hasNonEmptyValue(rawSecondFromSetting)) {
  secondScheduleTime = normalizeTime(rawSecondFromSetting);
 } else {
  secondScheduleTime = null;
 }
 const minIntervalRaw = Number(getSetting('minIntervalMinutes'));
 const minIntervalMinutes = Number.isFinite(minIntervalRaw) && minIntervalRaw > 0
  ? Math.floor(minIntervalRaw)
  : DEFAULT_BROADCAST_INTERVAL_MINUTES;
 const enabledRaw = getSetting('scheduleEnabled');
 const enabled = enabledRaw === null ? true : enabledRaw !== '0';
 return {
  scheduleTime,
  secondScheduleTime,
  minIntervalMinutes,
  enabled
 };
}

function hasNonEmptyValue(value) {
 return String(value ?? '').trim() !== '';
}

function sleep(ms) {
 const delayMs = Number(ms);
 return new Promise((resolve) => setTimeout(resolve, Number.isFinite(delayMs) && delayMs > 0 ? delayMs : 0));
}

function sanitizeTelegramErrorMessage(message) {
 const raw = String(message ?? '').trim();
 if (!raw) return 'Telegram request failed';
 const masked = raw
  .replace(/(https:\/\/api\.telegram\.org\/bot)[^/\s]+/gi, '$1<redacted>')
  .replace(/bot\d+:[A-Za-z0-9_-]+/g, 'bot<redacted>');
 if (/reason:\s*$/i.test(masked)) {
  return `${masked} network error`;
 }
 return masked;
}

function formatTelegramApiError(method, message) {
 const safeMessage = sanitizeTelegramErrorMessage(message);
 return `Telegram ${method} failed: ${safeMessage}`;
}

function isRetryableTelegramStatus(status) {
 return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

function isRetryableTelegramNetworkError(error) {
 const code = String(error?.code || error?.cause?.code || '').toUpperCase();
 if ([
  'ECONNRESET',
  'ETIMEDOUT',
  'ECONNREFUSED',
  'EPIPE',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'EAI_AGAIN',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_SOCKET'
 ].includes(code)) {
  return true;
 }
 const name = String(error?.name || '').toLowerCase();
 if (name.includes('abort')) return true;
 const message = String(error?.message || '').toLowerCase();
 return (
  message.includes('network') ||
  message.includes('socket') ||
  message.includes('timed out') ||
  message.includes('econnreset') ||
  message.includes('eai_again') ||
  message.includes('fetch failed') ||
  (message.includes('request to') && message.includes('failed'))
 );
}

function getTelegramRetryDelayMs(attempt, retryAfterSeconds) {
 const retryAfterMs = Number(retryAfterSeconds) * 1000;
 if (Number.isFinite(retryAfterMs) && retryAfterMs > 0) {
  return Math.min(Math.max(retryAfterMs, 500), TELEGRAM_API_RETRY_MAX_MS);
 }
 const jitterMs = Math.floor(Math.random() * 200);
 const expDelay = TELEGRAM_API_RETRY_BASE_MS * (2 ** Math.max(0, Number(attempt) || 0));
 return Math.min(expDelay + jitterMs, TELEGRAM_API_RETRY_MAX_MS);
}

async function telegramApi(method, payload = {}) {
 if (!process.env.TELEGRAM_BOT_TOKEN) {
  throw new Error('Telegram bot token not set');
 }
 const url = `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/${method}`;
 const attempts = Math.max(1, TELEGRAM_API_MAX_RETRIES + 1);
 for (let attempt = 0; attempt < attempts; attempt += 1) {
  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => controller.abort(), TELEGRAM_API_TIMEOUT_MS);
  let response;
  try {
   response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal: controller.signal
   });
  } catch (error) {
   const shouldRetry = attempt < attempts - 1 && isRetryableTelegramNetworkError(error);
   if (shouldRetry) {
    await sleep(getTelegramRetryDelayMs(attempt));
    continue;
   }
   throw new Error(formatTelegramApiError(method, error?.message || error));
  } finally {
   clearTimeout(timeoutHandle);
  }

  const data = await response.json().catch(() => ({}));
  if (response.ok && data.ok) {
   return data.result;
  }

  const status = Number(response.status) || 0;
  const errorCode = Number(data?.error_code) || 0;
  const shouldRetry = attempt < attempts - 1 && (isRetryableTelegramStatus(status) || errorCode === 429);
  if (shouldRetry) {
   await sleep(getTelegramRetryDelayMs(attempt, data?.parameters?.retry_after));
   continue;
  }
  const message = normalizeText(data?.description)
   || normalizeText(data?.error)
   || normalizeText(response.statusText)
   || `Telegram API error (${status || 'unknown'})`;
  throw new Error(formatTelegramApiError(method, message));
 }
 throw new Error(formatTelegramApiError(method, 'Telegram request failed after retries'));
}

async function downloadTelegramFile(fileId, nameHint) {
 const info = await telegramApi('getFile', { file_id: fileId });
 if (!info.file_path) return null;
 const fileUrl = `https://api.telegram.org/file/bot${process.env.TELEGRAM_BOT_TOKEN}/${info.file_path}`;
 const response = await fetch(fileUrl);
 if (!response.ok) {
  throw new Error(`Telegram file download failed: ${response.status}`);
 }
 const buffer = Buffer.from(await response.arrayBuffer());
 const ext = path.extname(info.file_path) || (nameHint ? path.extname(nameHint) : '');
 const safeExt = ext && ext.length <= 8 ? ext : '';
 const fileName = `${Date.now()}_${fileId}${safeExt}`;
 const destPath = path.join(uploadsDir, fileName);
 fs.writeFileSync(destPath, buffer);
 return { fileName, filePath: `uploads/${fileName}` };
}

function parseTelegramMessageId(value) {
 const candidate = typeof value === 'object' && value !== null ? value.message_id : value;
 const numeric = Number(candidate);
 if (!Number.isFinite(numeric) || numeric <= 0) return null;
 return Math.floor(numeric);
}

function parseTelegramViews(value) {
 const numeric = Number(value);
 if (!Number.isFinite(numeric) || numeric < 0) return null;
 return Math.floor(numeric);
}

function normalizeChatIdForStorage(value) {
 const normalized = normalizeText(value);
 if (!normalized) return null;
 return /^-?\d+$/.test(normalized) ? normalized : null;
}

function updateLogViewsFromTelegramMessage(message) {
 if (!message || !message.message_id) return;
 const messageId = parseTelegramMessageId(message.message_id);
 const chatId = normalizeChatIdForStorage(message?.chat?.id);
 const views = parseTelegramViews(message.views);
 if (!messageId || !chatId || views === null) return;
 const updatedAt = new Date().toISOString();
 const updateResult = db.prepare(`
  UPDATE logs
   SET sentViews = ?,
    viewsUpdatedAt = ?,
    sentMessageId = COALESCE(sentMessageId, ?),
    sentChatId = COALESCE(sentChatId, ?)
   WHERE platform = 'telegram'
    AND sentMessageId = ?
    AND (sentChatId = ? OR sentChatId IS NULL)
 `).run(views, updatedAt, messageId, chatId, messageId, chatId);
 if (updateResult.changes) {
  invalidateDerivedCaches({ forecast: false });
 }
}

async function handleTelegramMessage(message) {
 if (!message || !message.chat || !message.message_id) return;
 updateLogViewsFromTelegramMessage(message);
 const chatId = String(message.chat.id);
 const messageId = message.message_id;
 const text = normalizeText(message.text);
 const caption = normalizeText(message.caption);
 const markupButtons = extractButtonsFromReplyMarkup(message.reply_markup);
 const entitySourceText = message.caption || message.text || '';
 const entityList = Array.isArray(message.caption_entities) && message.caption_entities.length
  ? message.caption_entities
  : Array.isArray(message.entities) ? message.entities : [];
 const entityButtons = extractButtonsFromEntities(entitySourceText, entityList);
 const parsedButtons = markupButtons.length ? markupButtons : entityButtons;
 const buttonsJson = parsedButtons.length ? JSON.stringify(parsedButtons) : null;
 let mediaType = 'text';
 let fileId = null;
 let fileUniqueId = null;
 let fileName = null;
 let mimeType = null;

 if (message.photo && message.photo.length) {
  const largest = message.photo[message.photo.length - 1];
  mediaType = 'photo';
  fileId = largest.file_id;
  fileUniqueId = largest.file_unique_id;
 } else if (message.video) {
  mediaType = 'video';
  fileId = message.video.file_id;
  fileUniqueId = message.video.file_unique_id;
  fileName = message.video.file_name || null;
  mimeType = message.video.mime_type || null;
 } else if (message.document) {
  mediaType = 'document';
  fileId = message.document.file_id;
  fileUniqueId = message.document.file_unique_id;
  fileName = message.document.file_name || null;
  mimeType = message.document.mime_type || null;
 }

 // Ignore service/empty updates (no text, no caption, no supported media).
 if (!text && !caption && !fileId) return null;

 const createdAt = new Date((message.date || Math.floor(Date.now() / 1000)) * 1000).toISOString();
 const existing = db.prepare('SELECT id, filePath FROM drafts WHERE chatId = ? AND messageId = ?')
  .get(chatId, messageId);

 let filePath = existing ? existing.filePath : null;
 if (fileId && (mediaType === 'photo' || mediaType === 'video') && !filePath) {
  try {
   const download = await downloadTelegramFile(fileId, fileName);
   if (download) {
    filePath = download.filePath;
    fileName = download.fileName;
   }
  } catch (e) {
   console.log('Draft download failed', e.message);
  }
 }

 if (existing) {
  db.prepare(`UPDATE drafts SET
   mediaType = ?, text = ?, caption = ?, fileId = ?, fileUniqueId = ?, fileName = ?, mimeType = ?, filePath = ?, buttons = ?, createdAt = ?
   WHERE id = ?
  `).run(mediaType, text, caption, fileId, fileUniqueId, fileName, mimeType, filePath, buttonsJson, createdAt, existing.id);
  return existing.id;
 }

 const result = db.prepare(`INSERT INTO drafts
  (source, chatId, messageId, mediaType, text, caption, fileId, fileUniqueId, fileName, mimeType, filePath, buttons, createdAt)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
 `).run('telegram', chatId, messageId, mediaType, text, caption, fileId, fileUniqueId, fileName, mimeType, filePath, buttonsJson, createdAt);
 return result.lastInsertRowid;
}

async function pullTelegramUpdates() {
 const offsetValue = Number(getSetting('telegramUpdateOffset') || 0);
 const payload = {
  allowed_updates: ['message', 'channel_post']
 };
 if (offsetValue) payload.offset = offsetValue + 1;
 const updates = await telegramApi('getUpdates', payload);
 let maxUpdate = offsetValue;
 for (const update of updates) {
  if (update.update_id && update.update_id > maxUpdate) maxUpdate = update.update_id;
  const message = update.channel_post || update.message;
  if (message) {
   await handleTelegramMessage(message);
  }
 }
 if (maxUpdate > offsetValue) setSetting('telegramUpdateOffset', String(maxUpdate));
 return updates.length;
}

async function sendTelegramPost(post) {
 const chatId = normalizeText(process.env.TELEGRAM_CHANNEL_ID);
 if (!chatId) throw new Error('TELEGRAM_CHANNEL_ID is required');
 const replyMarkup = buildTelegramReplyMarkup(post);

 if (post.draftId) {
  const draft = db.prepare('SELECT * FROM drafts WHERE id = ?').get(post.draftId);
  if (!draft) throw new Error('Draft not found');
  const payload = {
   chat_id: chatId,
   from_chat_id: draft.chatId,
   message_id: draft.messageId
  };
  if (replyMarkup) payload.reply_markup = replyMarkup;
  const copyResult = await telegramApi('copyMessage', payload);
  return {
   sentChatId: normalizeChatIdForStorage(chatId),
   sentMessageId: parseTelegramMessageId(copyResult),
   sentViews: parseTelegramViews(copyResult?.views)
  };
 }

 if (!post.text) throw new Error('Post text is required');
 const payload = { chat_id: chatId, text: post.text };
 if (replyMarkup) payload.reply_markup = replyMarkup;
 const sentResult = await telegramApi('sendMessage', payload);
 const sentChatIdRaw = sentResult?.chat?.id !== undefined && sentResult?.chat?.id !== null
  ? String(sentResult.chat.id)
  : chatId;
 return {
  sentChatId: normalizeChatIdForStorage(sentChatIdRaw) || normalizeChatIdForStorage(chatId),
  sentMessageId: parseTelegramMessageId(sentResult),
  sentViews: parseTelegramViews(sentResult?.views)
 };
}

function insertLog(post, status, error, trigger, dateOverride, deliveryMeta) {
 const createdAt = new Date().toISOString();
 const date = dateOverride || createdAt.slice(0, 10);
 const normalizedCompanyId = Number(post?.companyId || 0) || null;
 const companyName = normalizeText(post?.companyName) || null;
 const sentChatId = normalizeText(deliveryMeta?.sentChatId) || null;
 const sentMessageId = parseTelegramMessageId(deliveryMeta?.sentMessageId);
 const sentViews = parseTelegramViews(deliveryMeta?.sentViews);
 db.prepare(`INSERT INTO logs
  (postId,companyId,companyName,platform,date,status,error,createdAt,trigger,publishedAt,sentChatId,sentMessageId,sentViews,viewsUpdatedAt)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
 `).run(
  post.id,
  normalizedCompanyId,
  companyName,
  post.platform,
  date,
  status,
  error || null,
  createdAt,
  trigger || 'schedule',
  createdAt,
  sentChatId,
 sentMessageId,
 sentViews,
 sentViews !== null ? createdAt : null
 );
 invalidateDerivedCaches();
}

function toHourLabel(value) {
 if (!value) return null;
 const parsed = new Date(value);
 if (Number.isNaN(parsed.getTime())) return null;
 const hh = String(parsed.getHours()).padStart(2, '0');
 return `${hh}:00`;
}

function toDatePart(value) {
 const text = String(value || '');
 if (text.length < 10) return null;
 const datePart = text.slice(0, 10);
 return isValidDateString(datePart) ? datePart : null;
}

let analyticsReportCache = {
 payload: null,
 expiresAt: 0
};
let scheduleForecastCache = new Map();
let scheduleForecastVersion = 1;

function invalidateAnalyticsReportCache() {
 analyticsReportCache = {
  payload: null,
  expiresAt: 0
 };
}

function invalidateScheduleForecastCache() {
 scheduleForecastVersion += 1;
 scheduleForecastCache.clear();
}

function invalidateDerivedCaches(options = {}) {
 const invalidateAnalytics = options.analytics !== false;
 const invalidateForecast = options.forecast !== false;
 if (invalidateAnalytics) {
  invalidateAnalyticsReportCache();
 }
 if (invalidateForecast) {
  invalidateScheduleForecastCache();
 }
}

function getCachedCompanyAnalyticsReport() {
 const now = Date.now();
 if (analyticsReportCache.payload && analyticsReportCache.expiresAt > now) {
  return analyticsReportCache.payload;
 }
 const payload = buildCompanyAnalyticsReport();
 analyticsReportCache = {
  payload,
  expiresAt: now + ANALYTICS_CACHE_TTL_MS
 };
 return payload;
}

function getCachedScheduleForecast(targetDate, config, options = {}) {
 const startFromNow = Boolean(options.startFromNow);
 const forceImmediateSession = Boolean(options.forceImmediateSession);
 const key = [
  scheduleForecastVersion,
  targetDate,
  config.scheduleTime,
  config.secondScheduleTime,
  Number(config.minIntervalMinutes) || 0,
  startFromNow ? 1 : 0,
  forceImmediateSession ? 1 : 0
 ].join('|');
 const now = Date.now();
 const cached = scheduleForecastCache.get(key);
 if (cached && cached.expiresAt > now) {
  return cached.payload;
 }
 if (cached) {
  scheduleForecastCache.delete(key);
 }

 const posts = getActiveTelegramPostsForDate(targetDate);
 const payload = calculateScheduleForecast(targetDate, posts, config, {
  startFromNow,
  forceImmediateSession
 });
 scheduleForecastCache.set(key, {
  payload,
  expiresAt: now + FORECAST_CACHE_TTL_MS
 });
 while (scheduleForecastCache.size > FORECAST_CACHE_MAX_ITEMS) {
  const oldestKey = scheduleForecastCache.keys().next().value;
  if (oldestKey === undefined) break;
  scheduleForecastCache.delete(oldestKey);
 }
 return payload;
}

function buildCompanyAnalyticsReport() {
 const companies = db.prepare(`
  SELECT id, name
  FROM companies
  ORDER BY LOWER(name) ASC, id ASC
 `).all();

 const adStartRows = db.prepare(`
  SELECT companyId, MIN(startDate) as adStartDate
  FROM posts
  WHERE platform = 'telegram'
  GROUP BY companyId
 `).all();

 const adStartMap = new Map();
 for (const row of adStartRows) {
  const companyId = Number(row?.companyId || 0);
  if (!companyId) continue;
  const adStartDate = isValidDateString(row?.adStartDate) ? row.adStartDate : null;
  adStartMap.set(companyId, adStartDate);
 }

 const statsMap = new Map();
 for (const company of companies) {
  const companyId = Number(company?.id || 0);
  if (!companyId) continue;
  statsMap.set(companyId, {
   companyId,
   companyName: String(company?.name || `#${companyId}`),
   adStartDate: adStartMap.get(companyId) || null,
   totalPublications: 0,
   totalViews: 0,
   publicationsWithViews: 0,
   hotHourPublications: 0,
   hotHourViews: 0
  });
 }

 const sentLogs = db.prepare(`
  SELECT
   logs.id,
   logs.postId,
   logs.status,
   logs.createdAt,
   logs.publishedAt,
   logs.sentViews,
   COALESCE(logs.companyId, posts.companyId) as companyId,
   COALESCE(logs.companyName, companies.name) as companyName
  FROM logs
  LEFT JOIN posts ON logs.postId = posts.id
  LEFT JOIN companies ON COALESCE(logs.companyId, posts.companyId) = companies.id
  WHERE logs.platform = 'telegram' AND logs.status = 'sent'
  ORDER BY logs.id ASC
 `).all();

 const hourMap = new Map();
 const publicationEvents = [];
 let hasCapturedViews = false;

 for (const row of sentLogs) {
  const companyId = Number(row?.companyId || 0);
  if (!companyId) continue;
  const publishedAt = row?.publishedAt || row?.createdAt || null;
  const publishedDate = toDatePart(publishedAt);
  const adStartDate = adStartMap.get(companyId) || null;
  if (adStartDate && publishedDate && publishedDate < adStartDate) {
   continue;
  }

  if (!statsMap.has(companyId)) {
   statsMap.set(companyId, {
    companyId,
    companyName: String(row?.companyName || `#${companyId}`),
    adStartDate,
    totalPublications: 0,
    totalViews: 0,
    publicationsWithViews: 0,
    hotHourPublications: 0,
    hotHourViews: 0
   });
  }

  const stat = statsMap.get(companyId);
  const sentViews = parseTelegramViews(row?.sentViews);
  const hour = toHourLabel(publishedAt);

  stat.totalPublications += 1;
  if (sentViews !== null) {
   stat.totalViews += sentViews;
   stat.publicationsWithViews += 1;
   hasCapturedViews = true;
  }

  if (hour) {
   if (!hourMap.has(hour)) {
    hourMap.set(hour, {
     hour,
     totalViews: 0,
     totalPublications: 0
    });
   }
   const hourRow = hourMap.get(hour);
   hourRow.totalPublications += 1;
   if (sentViews !== null) {
    hourRow.totalViews += sentViews;
   }
   publicationEvents.push({
    companyId,
    hour,
    sentViews: sentViews || 0
   });
  }
 }

 const hourRows = Array.from(hourMap.values());
 let reachMetric = hasCapturedViews ? 'views' : 'publications';
 const maxViews = hourRows.reduce((maxValue, row) => Math.max(maxValue, Number(row?.totalViews || 0)), 0);
 if (reachMetric === 'views' && maxViews <= 0) {
  reachMetric = 'publications';
 }

 const withReach = hourRows.map((row) => {
  const reachValue = reachMetric === 'views'
   ? Number(row?.totalViews || 0)
   : Number(row?.totalPublications || 0);
  return {
   hour: row.hour,
   totalViews: Number(row?.totalViews || 0),
   totalPublications: Number(row?.totalPublications || 0),
   reachValue
  };
 });

 withReach.sort((a, b) => {
  if (b.reachValue !== a.reachValue) return b.reachValue - a.reachValue;
  if (b.totalPublications !== a.totalPublications) return b.totalPublications - a.totalPublications;
  return String(a.hour || '').localeCompare(String(b.hour || ''), 'en');
 });

 const hottestReachValue = withReach.length ? withReach[0].reachValue : 0;
 const hotHours = hottestReachValue > 0
  ? withReach.filter((row) => row.reachValue === hottestReachValue)
  : [];
 const hotHourSet = new Set(hotHours.map((row) => row.hour));

 for (const event of publicationEvents) {
  if (!hotHourSet.has(event.hour)) continue;
  const stat = statsMap.get(event.companyId);
  if (!stat) continue;
  stat.hotHourPublications += 1;
  stat.hotHourViews += Number(event.sentViews || 0);
 }

 const companiesList = Array.from(statsMap.values()).map((row) => {
  const totalPublications = Number(row.totalPublications || 0);
  const totalViews = Number(row.totalViews || 0);
  const hotHourPublications = Number(row.hotHourPublications || 0);
  const hotHourViews = Number(row.hotHourViews || 0);
  const totalReachValue = reachMetric === 'views' ? totalViews : totalPublications;
  const hotHourReachValue = reachMetric === 'views' ? hotHourViews : hotHourPublications;
  return {
   companyId: row.companyId,
   companyName: row.companyName,
   adStartDate: row.adStartDate || null,
   totalPublications,
   totalViews,
   totalReachValue,
   averageViewsPerPublication: totalPublications ? Number((totalViews / totalPublications).toFixed(2)) : 0,
   publicationsWithViews: Number(row.publicationsWithViews || 0),
   viewCoveragePercent: totalPublications
    ? Math.round((Number(row.publicationsWithViews || 0) / totalPublications) * 100)
    : 0,
   hotHourPublications,
   hotHourViews,
   hotHourReachValue
  };
 });

 companiesList.sort((a, b) => {
  if (b.totalReachValue !== a.totalReachValue) return b.totalReachValue - a.totalReachValue;
  if (b.totalPublications !== a.totalPublications) return b.totalPublications - a.totalPublications;
  return String(a.companyName || '').localeCompare(String(b.companyName || ''), 'en', { sensitivity: 'base' });
 });

 const totals = companiesList.reduce((acc, row) => {
  acc.totalPublications += Number(row.totalPublications || 0);
  acc.totalViews += Number(row.totalViews || 0);
  return acc;
 }, { totalPublications: 0, totalViews: 0 });

 return {
  generatedAt: new Date().toISOString(),
  reachMetric,
  reachMetricLabel: reachMetric === 'views' ? 'telegram_views' : 'publication_count',
  totals: {
   companies: companiesList.length,
   totalPublications: totals.totalPublications,
   totalViews: totals.totalViews,
   totalReachValue: reachMetric === 'views' ? totals.totalViews : totals.totalPublications,
   hotHoursCount: hotHours.length
  },
  hotHours,
  topHours: withReach.slice(0, 12),
  companies: companiesList
 };
}

function getActiveTelegramPostsForDate(targetDate) {
 return db.prepare(`
  SELECT posts.*,
    companies.name as companyName,
    COALESCE(companies.premium, 0) as companyPremium,
    companies.preferredTime,
    links.code as linkCode
   FROM posts
   JOIN companies ON posts.companyId = companies.id
   LEFT JOIN links ON posts.linkId = links.id
   WHERE posts.active=1 AND posts.startDate<=? AND posts.endDate>=? AND posts.platform = 'telegram'
   ORDER BY LOWER(companies.name) ASC, posts.id ASC
  `).all(targetDate, targetDate);
}

function minutesToTimeString(minutes) {
 if (!Number.isFinite(minutes)) return null;
 const normalized = Math.max(0, Math.floor(minutes));
 const hh = Math.floor(normalized / 60) % 24;
 const mm = normalized % 60;
 return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
}

function compareCompanyPosts(a, b) {
 const nameA = String(a?.companyName || '');
 const nameB = String(b?.companyName || '');
 const byName = nameA.localeCompare(nameB, 'en', { sensitivity: 'base' });
 if (byName) return byName;
 return Number(a?.id || 0) - Number(b?.id || 0);
}

function hashString32(value) {
 const text = String(value || '');
 let hash = 2166136261;
 for (let i = 0; i < text.length; i += 1) {
  hash ^= text.charCodeAt(i);
  hash = Math.imul(hash, 16777619);
 }
 return hash >>> 0;
}

function createSeededRandom(seed) {
 let state = (Number(seed) >>> 0) || 1;
 return () => {
  state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
  return state / 0x100000000;
 };
}

function shufflePostsForSession(posts, targetDate, sessionIndex) {
 const items = [...(posts || [])];
 if (items.length <= 1) return items;
 const seed = hashString32(`${targetDate}|session:${sessionIndex}|posts:${items.length}`);
 const random = createSeededRandom(seed);
 for (let i = items.length - 1; i > 0; i -= 1) {
  const j = Math.floor(random() * (i + 1));
  [items[i], items[j]] = [items[j], items[i]];
 }
 return items;
}

function rotatePosts(posts, shift = 1) {
 const items = [...(posts || [])];
 if (items.length <= 1) return items;
 const normalizedShift = ((Math.floor(shift) % items.length) + items.length) % items.length;
 if (!normalizedShift) return items;
 return items.slice(normalizedShift).concat(items.slice(0, normalizedShift));
}

function postOrderSignature(posts) {
 return (posts || []).map((post) => Number(post?.id || 0)).join(',');
}

function formatPlanMinuteLabel(minutes) {
 if (!Number.isFinite(minutes)) return null;
 const normalized = Math.max(0, Math.floor(minutes));
 const dayOffset = Math.floor(normalized / (24 * 60));
 const time = minutesToTimeString(normalized) || null;
 if (!time) return null;
 if (dayOffset <= 0) return time;
 return `${time} (+${dayOffset}d)`;
}

function getSessionStartRows(config) {
 const unique = new Set();
 const rows = [];
 const timeValues = [config?.scheduleTime, config?.secondScheduleTime];
 for (const raw of timeValues) {
  const normalized = normalizeTime(raw);
  const minutes = timeToMinutes(normalized);
  if (minutes === null) continue;
  if (unique.has(minutes)) continue;
  unique.add(minutes);
  rows.push({ configuredTime: normalized, configuredMinutes: minutes });
 }
 rows.sort((a, b) => a.configuredMinutes - b.configuredMinutes);
 return rows.map((row, index) => ({
  sessionIndex: index + 1,
  configuredTime: row.configuredTime,
  configuredMinutes: row.configuredMinutes
 }));
}

function buildRuntimePlan(targetDate, posts, config, options = {}) {
 const immediateGraceMinutes = Math.max(1, Number(config?.minIntervalMinutes) || DEFAULT_BROADCAST_INTERVAL_MINUTES);
 const intervalMinutes = 0;
 const sortedPosts = [...(posts || [])].sort(compareCompanyPosts);
 const perPostCounts = new Map(sortedPosts.map((post) => [Number(post?.id || 0), 0]));
 const sessionRows = getSessionStartRows(config);
 const nowMinutes = (options.startFromNow && targetDate === getCurrentDateString())
  ? getCurrentMinutes()
  : null;
 const forceImmediateSession = Boolean(options.forceImmediateSession);
 const maxDayMinutes = 24 * 60;

 const sessions = [];
 const slots = [];
 let immediateSessionUsed = false;
 let previousSessionOrder = null;

 for (const sessionRow of sessionRows) {
  let startMinutes = sessionRow.configuredMinutes;
  let mode = 'scheduled';
  if (Number.isFinite(nowMinutes) && startMinutes < nowMinutes) {
   if ((nowMinutes - startMinutes) <= immediateGraceMinutes) {
    startMinutes = nowMinutes;
    mode = 'immediate';
   } else if (forceImmediateSession && !immediateSessionUsed) {
    startMinutes = nowMinutes;
    mode = 'immediate';
    immediateSessionUsed = true;
   } else {
    sessions.push({
      sessionIndex: sessionRow.sessionIndex,
      configuredStartTime: sessionRow.configuredTime,
      configuredStartMinutes: sessionRow.configuredMinutes,
      startTime: sessionRow.configuredTime,
      startMinutes: sessionRow.configuredMinutes,
      endTime: null,
      endMinutes: null,
      endLabel: null,
      plannedPublications: 0,
      overflowSkipped: sortedPosts.length,
      mode: 'skipped_past'
    });
    continue;
   }
  }

  let postsForSession = shufflePostsForSession(sortedPosts, targetDate, sessionRow.sessionIndex);
  if (postsForSession.length > 1) {
   const currentOrder = postOrderSignature(postsForSession);
   if (previousSessionOrder && currentOrder === previousSessionOrder) {
    postsForSession = rotatePosts(postsForSession, sessionRow.sessionIndex || 1);
   }
  }
  previousSessionOrder = postOrderSignature(postsForSession);

  let plannedPublications = 0;
  let overflowSkipped = 0;
  for (const post of postsForSession) {
   const candidateMinutes = startMinutes;
   if (candidateMinutes >= maxDayMinutes) {
    overflowSkipped += 1;
    continue;
   }
   const postId = Number(post?.id || 0);
   if (!postId) continue;
   slots.push({
    post,
    minutes: candidateMinutes,
    sessionIndex: sessionRow.sessionIndex,
    source: mode === 'immediate' ? 'immediate' : 'session'
   });
   perPostCounts.set(postId, (perPostCounts.get(postId) || 0) + 1);
   plannedPublications += 1;
  }

  const endMinutes = plannedPublications > 0
   ? startMinutes
   : null;
  sessions.push({
   sessionIndex: sessionRow.sessionIndex,
   configuredStartTime: sessionRow.configuredTime,
   configuredStartMinutes: sessionRow.configuredMinutes,
   startTime: minutesToTimeString(startMinutes),
   startMinutes,
   endTime: endMinutes === null ? null : minutesToTimeString(endMinutes),
   endMinutes,
   endLabel: endMinutes === null ? null : formatPlanMinuteLabel(endMinutes),
   plannedPublications,
   overflowSkipped,
   mode
  });
 }

 slots.sort((a, b) => {
  if (a.minutes !== b.minutes) return a.minutes - b.minutes;
  if (a.sessionIndex !== b.sessionIndex) return a.sessionIndex - b.sessionIndex;
  return compareCompanyPosts(a.post, b.post);
 });

 const totalPublications = slots.length;
 const estimatedEndMinutes = totalPublications ? slots[slots.length - 1].minutes : null;

 return {
  sortedPosts,
  perPostCounts,
  slots,
  sessions,
  intervalMinutes,
  totalPublications,
  estimatedEndMinutes
 };
}

function calculateScheduleForecast(date, posts, config, options = {}) {
 const plan = buildRuntimePlan(date, posts, config, {
  startFromNow: Boolean(options.startFromNow),
  forceImmediateSession: Boolean(options.forceImmediateSession)
 });
 const perPost = plan.sortedPosts.map((post) => ({
  postId: Number(post?.id || 0),
  companyName: post?.companyName || null,
  publishCount: plan.perPostCounts.get(Number(post?.id || 0)) || 0
 }));
 const sessionSummaries = plan.sessions.map((session) => ({
  sessionIndex: session.sessionIndex,
  configuredStartTime: session.configuredStartTime,
  startTime: session.startTime,
  endTime: session.endTime,
  endLabel: session.endLabel,
  plannedPublications: session.plannedPublications,
  overflowSkipped: session.overflowSkipped,
  mode: session.mode
 }));
 const firstSession = sessionSummaries.find((session) => Number(session?.sessionIndex || 0) === 1)
  || sessionSummaries[0]
  || null;

 return {
  date,
  config: {
   scheduleTime: config.scheduleTime,
   secondScheduleTime: config.secondScheduleTime,
   minIntervalMinutes: plan.intervalMinutes
  },
  totals: {
   activePosts: perPost.length,
   totalPublications: plan.totalPublications,
   intervalMinutes: plan.intervalMinutes,
   sessions: sessionSummaries.length,
   firstSessionEndTime: firstSession?.endLabel || firstSession?.endTime || null,
   firstSessionPublications: Number(firstSession?.plannedPublications || 0),
   estimatedEndTime: formatPlanMinuteLabel(plan.estimatedEndMinutes)
  },
  sessions: sessionSummaries,
  perPost
 };
}

function generateDailySchedule(date, startFromNow = true, options = {}) {
 const targetDate = date || getCurrentDateString();
 const settings = getSchedulerSettings();
 const posts = getActiveTelegramPostsForDate(targetDate);
 const dayStart = `${targetDate}T00:00:00`;
 const dayEnd = `${targetDate}T23:59:59.999`;
 const nowIsoForDate = (startFromNow && targetDate === getCurrentDateString())
  ? nowLocalIso()
  : dayStart;

 const plan = buildRuntimePlan(targetDate, posts, settings, {
  startFromNow: Boolean(startFromNow),
  forceImmediateSession: Boolean(options.forceImmediateSession)
 });
 const createdAt = new Date().toISOString();
 const persistSchedule = db.transaction(() => {
  db.prepare(`
   DELETE FROM schedule_items
   WHERE status = 'pending'
    AND scheduledAt >= ?
    AND scheduledAt <= ?
  `).run(nowIsoForDate, dayEnd);

  let inserted = 0;
  for (const slot of plan.slots) {
   const scheduledAt = minutesToIso(targetDate, slot.minutes);
   if (!scheduledAt) continue;
   if (scheduledAt < nowIsoForDate || scheduledAt > dayEnd) continue;
   const existingNonPending = db.prepare(`
    SELECT id
    FROM schedule_items
    WHERE postId = ?
     AND scheduledAt = ?
     AND status <> 'pending'
    LIMIT 1
   `).get(slot.post.id, scheduledAt);
   if (existingNonPending?.id) continue;
   db.prepare(`
    INSERT INTO schedule_items (postId, scheduledAt, status, createdAt, sessionIndex)
    VALUES (?,?,?,?,?)
   `).run(slot.post.id, scheduledAt, 'pending', createdAt, Number(slot.sessionIndex) || null);
   inserted += 1;
  }
  return inserted;
 });
 const inserted = persistSchedule();

 return {
  date: targetDate,
  total: posts.length,
  scheduled: inserted,
  intervalMinutes: plan.intervalMinutes,
  estimatedEndTime: formatPlanMinuteLabel(plan.estimatedEndMinutes),
  sessions: plan.sessions.map((session) => ({
   sessionIndex: session.sessionIndex,
   startTime: session.startTime,
   endTime: session.endLabel || session.endTime,
   plannedPublications: session.plannedPublications,
   overflowSkipped: session.overflowSkipped,
   mode: session.mode
  }))
 };
}

function cleanupOrphanScheduleItems() {
 db.prepare(`
  DELETE FROM schedule_items
   WHERE postId NOT IN (SELECT id FROM posts)
 `).run();
}

function reclaimStaleProcessingItems(timeoutMinutes = 120) {
 const timeoutMs = Math.max(5, Number(timeoutMinutes) || 120) * 60 * 1000;
 const threshold = new Date(Date.now() - timeoutMs).toISOString();
 db.prepare(`
  UPDATE schedule_items
   SET status = 'pending',
    processingStartedAt = NULL,
    error = COALESCE(error, 'Recovered after process restart')
   WHERE status = 'processing'
    AND (processingStartedAt IS NULL OR processingStartedAt < ?)
 `).run(threshold);
}

function markExpiredPendingScheduleItems(currentDate) {
 if (!isValidDateString(currentDate)) return;
 const dayStart = `${currentDate}T00:00:00`;
 const updatedAt = new Date().toISOString();
 db.prepare(`
  UPDATE schedule_items
   SET status = 'failed',
    sentAt = COALESCE(sentAt, ?),
    error = COALESCE(error, 'Skipped: scheduled day passed'),
    processingStartedAt = NULL
   WHERE status = 'pending'
    AND scheduledAt < ?
 `).run(updatedAt, dayStart);
}

function markInvalidPendingScheduleItems() {
 const updatedAt = new Date().toISOString();
 db.prepare(`
  UPDATE schedule_items
   SET status = 'failed',
    sentAt = COALESCE(sentAt, ?),
    error = COALESCE(error, 'Skipped: post inactive or outside campaign date'),
    processingStartedAt = NULL
   WHERE status = 'pending'
    AND EXISTS (
     SELECT 1
     FROM posts
     WHERE posts.id = schedule_items.postId
      AND (
       posts.platform <> 'telegram'
       OR COALESCE(posts.active, 0) <> 1
       OR posts.startDate > substr(schedule_items.scheduledAt, 1, 10)
       OR posts.endDate < substr(schedule_items.scheduledAt, 1, 10)
      )
    )
 `).run(updatedAt);
}

function claimNextDueScheduleItem(nowIso, currentDate) {
 if (!isValidDateString(currentDate)) return null;
 const dayStart = `${currentDate}T00:00:00`;
 const claimStartedAt = new Date().toISOString();
 const claimTx = db.transaction(() => {
  const candidate = db.prepare(`
   SELECT schedule_items.id as scheduleId
   FROM schedule_items
   JOIN posts ON schedule_items.postId = posts.id
   WHERE schedule_items.status = 'pending'
    AND schedule_items.scheduledAt >= ?
    AND schedule_items.scheduledAt <= ?
    AND posts.platform = 'telegram'
    AND COALESCE(posts.active, 0) = 1
    AND posts.startDate <= substr(schedule_items.scheduledAt, 1, 10)
    AND posts.endDate >= substr(schedule_items.scheduledAt, 1, 10)
   ORDER BY schedule_items.scheduledAt ASC, schedule_items.id ASC
   LIMIT 1
  `).get(dayStart, nowIso);
  if (!candidate?.scheduleId) return null;
  const claimResult = db.prepare(`
   UPDATE schedule_items
    SET status = 'processing',
     processingStartedAt = ?,
     error = NULL
    WHERE id = ? AND status = 'pending'
  `).run(claimStartedAt, candidate.scheduleId);
  if (!claimResult.changes) return null;
  return candidate.scheduleId;
 });
 return claimTx();
}

let scheduleProcessing = false;
async function processDueSchedule(force = false) {
 if (scheduleProcessing) return;
 scheduleProcessing = true;
 try {
  const settings = getSchedulerSettings();
  if (!force && !settings.enabled) return;

  cleanupOrphanScheduleItems();
  reclaimStaleProcessingItems(Math.max(10, settings.minIntervalMinutes * 2));
  const currentDate = getCurrentDateString();
  markExpiredPendingScheduleItems(currentDate);
  markInvalidPendingScheduleItems();
  while (true) {
   const nowIso = nowLocalIso();
   const claimedScheduleId = claimNextDueScheduleItem(nowIso, currentDate);
   if (!claimedScheduleId) break;

   const item = db.prepare(`
    SELECT
     schedule_items.id as scheduleId,
     schedule_items.postId as schedulePostId,
     schedule_items.scheduledAt,
     schedule_items.status,
     schedule_items.error,
     schedule_items.createdAt,
     schedule_items.sentAt,
     schedule_items.processingStartedAt,
     posts.*,
     companies.name as companyName,
     links.code as linkCode
    FROM schedule_items
    LEFT JOIN posts ON schedule_items.postId = posts.id
    LEFT JOIN companies ON posts.companyId = companies.id
    LEFT JOIN links ON posts.linkId = links.id
    WHERE schedule_items.id = ?
    LIMIT 1
   `).get(claimedScheduleId);

   const sentAt = new Date().toISOString();
   if (!item?.scheduleId) {
    db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
     .run('failed', sentAt, 'Post not found', null, claimedScheduleId);
    continue;
   }
   if (!item.platform) {
    db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
     .run('failed', sentAt, 'Post not found', null, item.scheduleId);
    continue;
   }
   if (item.platform !== 'telegram') {
    db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
     .run('failed', sentAt, 'Unsupported platform', null, item.scheduleId);
    continue;
   }
   const scheduledDate = toDatePart(item.scheduledAt);
   const inCampaignWindow = Number(item.active) === 1 &&
    isValidDateString(item.startDate) &&
    isValidDateString(item.endDate) &&
    isValidDateString(scheduledDate) &&
    item.startDate <= scheduledDate &&
    item.endDate >= scheduledDate;
   if (!inCampaignWindow) {
    db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
     .run('failed', sentAt, 'Skipped: post inactive or outside campaign date', null, item.scheduleId);
    continue;
   }
   try {
    const deliveryMeta = await sendTelegramPost(item);
    db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
     .run('sent', sentAt, null, null, item.scheduleId);
    insertLog(item, 'sent', null, 'auto', null, deliveryMeta);
    setSetting('lastSentAt', sentAt);
   } catch (e) {
    db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
     .run('failed', sentAt, e.message, null, item.scheduleId);
    insertLog(item, 'failed', e.message, 'auto');
   }
  }
 } finally {
  scheduleProcessing = false;
 }
}

let scheduleJobs = [];
function configureScheduleJob() {
 for (const job of scheduleJobs) {
  try {
   job.stop();
  } catch (e) {}
 }
 scheduleJobs = [];
 const settings = getSchedulerSettings();
 if (!settings.enabled) return;
 const sessionStarts = getSessionStartRows(settings);
 for (const session of sessionStarts) {
  const [hh, mm] = session.configuredTime.split(':').map(Number);
  const cronExpr = `${mm} ${hh} * * *`;
  const job = cron.schedule(cronExpr, async () => {
   const activationDate = getSetting('scheduleActivationDate');
   const today = getCurrentDateString();
   if (isValidDateString(activationDate) && today < activationDate) {
    return;
   }
   generateDailySchedule(undefined, true);
   processDueSchedule(true).catch((e) => console.log('Session run failed:', e.message));
  }, CRON_TZ ? { timezone: CRON_TZ } : undefined);
  scheduleJobs.push(job);
 }
}

async function publishPostNow(postId) {
 const post = db.prepare(`
 SELECT posts.*, 
   companies.name as companyName,
   links.code as linkCode
  FROM posts
  JOIN companies ON posts.companyId = companies.id
  LEFT JOIN links ON posts.linkId = links.id
  WHERE posts.id = ?
 `).get(postId);

 if (!post) throw new Error('Post not found');

 try {
  if (post.platform !== 'telegram') throw new Error('Unsupported platform');
  const deliveryMeta = await sendTelegramPost(post);
  insertLog(post, 'sent', null, 'manual', null, deliveryMeta);
  return { ok: true, status: 'sent' };
 } catch (e) {
  insertLog(post, 'failed', e.message, 'manual');
  return { ok: false, status: 'failed', error: e.message };
 }
}

db.prepare(`CREATE TABLE IF NOT EXISTS companies (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 name TEXT,
 telegramChannelId TEXT,
 telegramPublicUrl TEXT,
 preferredTime TEXT,
 premium INTEGER DEFAULT 0
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS posts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 companyId INTEGER,
 text TEXT,
 platform TEXT,
 startDate TEXT,
 endDate TEXT,
 active INTEGER DEFAULT 1,
 draftId INTEGER,
 ctaUrl TEXT,
 ctaLabel TEXT,
 trackLinks INTEGER DEFAULT 1,
 linkId INTEGER,
 buttons TEXT
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS links (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 code TEXT UNIQUE,
 url TEXT,
 postId INTEGER,
 createdAt TEXT,
 clickCount INTEGER DEFAULT 0,
 lastClickedAt TEXT
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS link_clicks (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 linkId INTEGER,
 clickedAt TEXT,
 userAgent TEXT,
 referrer TEXT,
 ipHash TEXT
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS schedule_items (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 postId INTEGER,
 scheduledAt TEXT,
 status TEXT,
 error TEXT,
 createdAt TEXT,
 sentAt TEXT,
 processingStartedAt TEXT,
 sessionIndex INTEGER
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS logs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 postId INTEGER,
 companyId INTEGER,
 companyName TEXT,
 platform TEXT,
 date TEXT,
 status TEXT,
 error TEXT,
 createdAt TEXT,
 trigger TEXT,
 publishedAt TEXT,
 sentChatId TEXT,
 sentMessageId INTEGER,
 sentViews INTEGER,
 viewsUpdatedAt TEXT
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS drafts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 source TEXT,
 chatId TEXT,
 messageId INTEGER,
 mediaType TEXT,
 text TEXT,
 caption TEXT,
 fileId TEXT,
 fileUniqueId TEXT,
 fileName TEXT,
 mimeType TEXT,
 filePath TEXT,
 buttons TEXT,
 createdAt TEXT
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS settings (
 key TEXT PRIMARY KEY,
 value TEXT
)`).run();

db.prepare('CREATE INDEX IF NOT EXISTS idx_schedule_items_status_scheduled ON schedule_items(status, scheduledAt)').run();
db.prepare('CREATE INDEX IF NOT EXISTS idx_schedule_items_post ON schedule_items(postId)').run();

ensureColumn('companies', 'telegramChannelId', 'TEXT');
ensureColumn('companies', 'telegramPublicUrl', 'TEXT');
ensureColumn('companies', 'preferredTime', 'TEXT');
ensureColumn('companies', 'premium', 'INTEGER DEFAULT 0');

ensureColumn('posts', 'draftId', 'INTEGER');
ensureColumn('posts', 'ctaUrl', 'TEXT');
ensureColumn('posts', 'ctaLabel', 'TEXT');
ensureColumn('posts', 'trackLinks', 'INTEGER');
ensureColumn('posts', 'linkId', 'INTEGER');
ensureColumn('posts', 'buttons', 'TEXT');
ensureColumn('logs', 'createdAt', 'TEXT');
ensureColumn('logs', 'trigger', 'TEXT');
ensureColumn('logs', 'companyId', 'INTEGER');
ensureColumn('logs', 'companyName', 'TEXT');
ensureColumn('logs', 'publishedAt', 'TEXT');
ensureColumn('logs', 'sentChatId', 'TEXT');
ensureColumn('logs', 'sentMessageId', 'INTEGER');
ensureColumn('logs', 'sentViews', 'INTEGER');
ensureColumn('logs', 'viewsUpdatedAt', 'TEXT');
ensureColumn('schedule_items', 'processingStartedAt', 'TEXT');
ensureColumn('schedule_items', 'sessionIndex', 'INTEGER');
db.prepare('CREATE INDEX IF NOT EXISTS idx_logs_message_chat ON logs(platform, sentMessageId, sentChatId)').run();
db.prepare('CREATE INDEX IF NOT EXISTS idx_logs_platform_status_id ON logs(platform, status, id DESC)').run();
db.prepare('CREATE INDEX IF NOT EXISTS idx_logs_platform_date ON logs(platform, date)').run();

ensureColumn('drafts', 'source', 'TEXT');
ensureColumn('drafts', 'chatId', 'TEXT');
ensureColumn('drafts', 'messageId', 'INTEGER');
ensureColumn('drafts', 'mediaType', 'TEXT');
ensureColumn('drafts', 'text', 'TEXT');
ensureColumn('drafts', 'caption', 'TEXT');
ensureColumn('drafts', 'fileId', 'TEXT');
ensureColumn('drafts', 'fileUniqueId', 'TEXT');
ensureColumn('drafts', 'fileName', 'TEXT');
ensureColumn('drafts', 'mimeType', 'TEXT');
ensureColumn('drafts', 'filePath', 'TEXT');
ensureColumn('drafts', 'buttons', 'TEXT');
ensureColumn('drafts', 'createdAt', 'TEXT');

app.use('/uploads', express.static(uploadsDir));
cleanupOrphanScheduleItems();
markExpiredPendingScheduleItems(getCurrentDateString());
markInvalidPendingScheduleItems();
reclaimStaleProcessingItems(15);

app.get('/health', (_, res) => res.send({ ok: true }));
app.get('/runtime/time', (_, res) => res.send({
 currentDate: getCurrentDateString(),
 nowLocalIso: nowLocalIso(),
 cronTimeZone: CRON_TZ || null
}));

app.get('/settings', (_, res) => {
 const settings = getSchedulerSettings();
 const scheduleActivationDate = getSetting('scheduleActivationDate');
 res.send({
  scheduleEnabled: settings.enabled ? 1 : 0,
  scheduleTime: settings.scheduleTime,
  secondScheduleTime: settings.secondScheduleTime,
  minIntervalMinutes: settings.minIntervalMinutes,
  scheduleActivationDate: isValidDateString(scheduleActivationDate) ? scheduleActivationDate : null
 });
});

app.put('/settings', (req, res) => {
 const currentSettings = getSchedulerSettings();
 const scheduleEnabled = req.body?.scheduleEnabled;
 const runNow = req.body?.runNow === 1 || req.body?.runNow === true || req.body?.runNow === '1';
 const runDateInput = String(req.body?.runDate || '').trim();
 const runDate = runDateInput || getCurrentDateString();
 if (runNow && !isValidDateString(runDate)) return badRequest(res, 'Invalid run date');
 const scheduleTimeInput = req.body?.scheduleTime;
 const secondScheduleTimeInput = req.body?.secondScheduleTime;
 const minIntervalInput = req.body?.minIntervalMinutes;
 const scheduleTime = hasNonEmptyValue(scheduleTimeInput)
  ? normalizeTime(scheduleTimeInput)
  : currentSettings.scheduleTime;
 const hasSecondScheduleTimeInput = secondScheduleTimeInput !== undefined && secondScheduleTimeInput !== null;
 let secondScheduleTime = currentSettings.secondScheduleTime;
 if (hasSecondScheduleTimeInput) {
  secondScheduleTime = hasNonEmptyValue(secondScheduleTimeInput)
   ? normalizeTime(secondScheduleTimeInput)
   : null;
 }
 const minIntervalMinutes = hasNonEmptyValue(minIntervalInput)
  ? Number(minIntervalInput)
  : currentSettings.minIntervalMinutes;

 if (!scheduleTime) return badRequest(res, 'Invalid first start time');
 if (hasSecondScheduleTimeInput && hasNonEmptyValue(secondScheduleTimeInput) && !secondScheduleTime) {
  return badRequest(res, 'Invalid second start time');
 }
 if (secondScheduleTime && scheduleTime === secondScheduleTime) {
  return badRequest(res, 'First and second start time must be different');
 }
 if (!Number.isFinite(minIntervalMinutes) || minIntervalMinutes < 1 || minIntervalMinutes > 1440) {
  return badRequest(res, 'Invalid interval minutes');
 }

 setSetting('scheduleEnabled', scheduleEnabled === 0 || scheduleEnabled === false || scheduleEnabled === '0' ? '0' : '1');
 setSetting('scheduleTime', scheduleTime);
 setSetting('secondScheduleTime', secondScheduleTime || '');
 setSetting('minIntervalMinutes', String(Math.floor(minIntervalMinutes)));
 if (!runNow) {
  const tomorrow = addDaysToDateString(getCurrentDateString(), 1);
  if (tomorrow) {
   setSetting('scheduleActivationDate', tomorrow);
  }
  const nowIso = nowLocalIso();
  const todayEnd = `${getCurrentDateString()}T23:59:59.999`;
  db.prepare(`
   DELETE FROM schedule_items
   WHERE status = 'pending'
    AND scheduledAt >= ?
    AND scheduledAt <= ?
  `).run(nowIso, todayEnd);
 } else {
  setSetting('scheduleActivationDate', runDate);
 }
 invalidateDerivedCaches({ analytics: false });

 configureScheduleJob();
 let runNowResult = null;
 if (runNow) {
  runNowResult = generateDailySchedule(runDate, true, { forceImmediateSession: true });
  processDueSchedule(true).catch((e) => console.log('Settings run-now failed:', e.message));
 }
 res.send({ ok: true, runNowResult });
});

app.get('/r/:code', (req, res) => {
 const code = String(req.params.code || '').trim();
 if (!code) return res.status(404).send('Not found');
 const link = db.prepare('SELECT * FROM links WHERE code = ?').get(code);
 if (!link) return res.status(404).send('Not found');
 const now = new Date().toISOString();
 const userAgent = req.headers['user-agent'] || null;
 const referrer = req.headers['referer'] || req.headers['referrer'] || null;
 const ipRaw = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim();
 const ip = ipRaw || req.socket?.remoteAddress || '';
 const ipHash = hashIp(ip);
 db.prepare('UPDATE links SET clickCount = clickCount + 1, lastClickedAt = ? WHERE id = ?')
  .run(now, link.id);
 db.prepare('INSERT INTO link_clicks (linkId, clickedAt, userAgent, referrer, ipHash) VALUES (?,?,?,?,?)')
  .run(link.id, now, userAgent, referrer, ipHash);
 res.setHeader('Cache-Control', 'no-store');
 res.redirect(link.url);
});

app.get('/companies', (_, res) => res.send(db.prepare(`
 SELECT
  companies.id,
  companies.name,
  companies.preferredTime,
  COALESCE(companies.premium, 0) as premium,
  (SELECT COUNT(*) FROM posts WHERE posts.companyId = companies.id AND posts.platform = 'telegram') as postCount
 FROM companies
 ORDER BY LOWER(companies.name) ASC, companies.id ASC
`).all()));

app.post('/companies', (req, res) => {
 const name = normalizeText(req.body.name);
 if (!name) return badRequest(res, 'Company name is required');
 const preferredTime = normalizeTime(req.body.preferredTime);
 const premium = normalizePremiumFlag(req.body.premium);
 if (req.body.preferredTime && !preferredTime) return badRequest(res, 'Invalid preferred time');

 db.prepare(`INSERT INTO companies (name, preferredTime, premium)
  VALUES (?,?,?)
 `).run(name, preferredTime, premium);
 invalidateDerivedCaches();
 res.send({ ok: true });
});

app.put('/companies/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid company id');
 const name = normalizeText(req.body.name);
 if (!name) return badRequest(res, 'Company name is required');
 const preferredTime = normalizeTime(req.body.preferredTime);
 const premium = normalizePremiumFlag(req.body.premium);
 if (req.body.preferredTime && !preferredTime) return badRequest(res, 'Invalid preferred time');

 db.prepare(`UPDATE companies SET
  name=?, preferredTime=?, premium=?
  WHERE id=?
 `).run(name, preferredTime, premium, id);
 invalidateDerivedCaches();
 res.send({ ok: true });
});

app.delete('/companies/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid company id');
 const count = db.prepare('SELECT COUNT(*) as count FROM posts WHERE companyId = ? AND platform = \'telegram\'').get(id).count;
 if (count > 0) return badRequest(res, 'Company has posts. Delete or move posts first.');
 db.prepare('DELETE FROM companies WHERE id = ?').run(id);
 invalidateDerivedCaches();
 res.send({ ok: true });
});

app.get('/posts', (_, res) => {
 const rows = db.prepare(`
  SELECT
   posts.id,
   posts.companyId,
   posts.text,
   posts.platform,
   posts.startDate,
   posts.endDate,
   posts.active,
   posts.draftId,
   posts.ctaUrl,
   posts.ctaLabel,
   posts.trackLinks,
   posts.linkId,
   posts.buttons,
   companies.name as companyName,
   COALESCE(companies.premium, 0) as companyPremium,
   drafts.mediaType as draftMediaType,
   drafts.text as draftText,
   drafts.caption as draftCaption,
   links.code as linkCode,
   links.url as linkUrl,
   links.clickCount as linkClickCount,
   links.lastClickedAt as linkLastClickedAt
  FROM posts
  LEFT JOIN companies ON posts.companyId = companies.id
  LEFT JOIN drafts ON posts.draftId = drafts.id
  LEFT JOIN links ON posts.linkId = links.id
  WHERE posts.platform = 'telegram'
  ORDER BY LOWER(companies.name) ASC, posts.id ASC
 `).all();
 const enriched = rows.map((row) => ({
  ...row,
  buttons: parseStoredButtons(row.buttons),
  trackedUrl: buildTrackedUrl(row.linkCode)
 }));
 res.send(enriched);
});

app.post('/posts', (req, res) => {
 const { companyId, text, startDate, endDate, active, draftId, ctaUrl, ctaLabel, trackLinks, buttons } = req.body;
 const normalizedCompanyId = Number(companyId);
 if (!normalizedCompanyId) return badRequest(res, 'Company is required');
 const platform = 'telegram';
 if (!allowedPlatforms.has(platform)) return badRequest(res, 'Invalid platform');
 if (!isValidDateString(startDate) || !isValidDateString(endDate)) {
  return badRequest(res, 'Invalid date format');
 }
 if (startDate > endDate) return badRequest(res, 'Start date must be before end date');
 const company = db.prepare('SELECT id FROM companies WHERE id = ?').get(normalizedCompanyId);
 if (!company) return badRequest(res, 'Company not found');

 const bodyText = normalizeText(text);
 const normalizedDraftId = draftId ? Number(draftId) : null;
 if (platform === 'telegram' && !normalizedDraftId && !bodyText) {
  return badRequest(res, 'Telegram post needs draft or text');
 }
 const activeValue = active === 0 ? 0 : 1;
 const normalizedCtaUrl = normalizeUrl(ctaUrl);
 let trackLinksValue = trackLinks === 0 || trackLinks === false || trackLinks === '0' ? 0 : 1;
 if (!normalizedCtaUrl) trackLinksValue = 0;
 let normalizedButtons;
 try {
  normalizedButtons = normalizePostButtonsInput(buttons);
 } catch (e) {
  return badRequest(res, e.message);
 }
 const serializedButtons = normalizedButtons.length ? JSON.stringify(normalizedButtons) : null;

 const insert = db.prepare(`INSERT INTO posts (companyId,text,platform,startDate,endDate,active,draftId,ctaUrl,ctaLabel,trackLinks,linkId,buttons)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
 `).run(
  normalizedCompanyId,
  bodyText,
  platform,
  startDate,
  endDate,
  activeValue,
  normalizedDraftId,
  normalizedCtaUrl,
  normalizeText(ctaLabel),
  trackLinksValue,
  null,
  serializedButtons
 );

 const postId = insert.lastInsertRowid;
 let linkId = null;
 if (trackLinksValue && normalizedCtaUrl) {
  linkId = ensurePostLink(postId, normalizedCtaUrl, null, true);
  if (linkId) {
   db.prepare('UPDATE posts SET linkId=? WHERE id=?').run(linkId, postId);
  }
 }

 invalidateDerivedCaches();
 res.send({ ok: true, id: postId, linkId });
});

app.put('/posts/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid post id');
 const { companyId, text, startDate, endDate, active, draftId, ctaUrl, ctaLabel, trackLinks, buttons } = req.body;
 const normalizedCompanyId = Number(companyId);
 if (!normalizedCompanyId) return badRequest(res, 'Company is required');
 const platform = 'telegram';
 if (!allowedPlatforms.has(platform)) return badRequest(res, 'Invalid platform');
 if (!isValidDateString(startDate) || !isValidDateString(endDate)) {
  return badRequest(res, 'Invalid date format');
 }
 if (startDate > endDate) return badRequest(res, 'Start date must be before end date');
 const company = db.prepare('SELECT id FROM companies WHERE id = ?').get(normalizedCompanyId);
 if (!company) return badRequest(res, 'Company not found');

 const bodyText = normalizeText(text);
 const normalizedDraftId = draftId ? Number(draftId) : null;
 if (platform === 'telegram' && !normalizedDraftId && !bodyText) {
  return badRequest(res, 'Telegram post needs draft or text');
 }
 const activeValue = active === 0 ? 0 : 1;
 const normalizedCtaUrl = normalizeUrl(ctaUrl);
 const existing = db.prepare('SELECT linkId, trackLinks, buttons FROM posts WHERE id = ?').get(id);
 if (!existing) return res.status(404).send({ ok: false, error: 'Post not found' });
 let trackLinksValue;
 if (trackLinks === undefined || trackLinks === null) {
  trackLinksValue = existing?.trackLinks ?? 1;
 } else {
  trackLinksValue = trackLinks === 0 || trackLinks === false || trackLinks === '0' ? 0 : 1;
 }
 if (!normalizedCtaUrl) trackLinksValue = 0;

 let linkId = null;
 if (trackLinksValue && normalizedCtaUrl) {
  linkId = ensurePostLink(id, normalizedCtaUrl, existing?.linkId, true);
 }
 let normalizedButtons;
 if (buttons === undefined) {
  normalizedButtons = parseStoredButtons(existing.buttons);
 } else {
  try {
   normalizedButtons = normalizePostButtonsInput(buttons);
  } catch (e) {
   return badRequest(res, e.message);
  }
 }
 const serializedButtons = normalizedButtons.length ? JSON.stringify(normalizedButtons) : null;

 db.prepare(`UPDATE posts SET companyId=?, text=?, platform=?, startDate=?, endDate=?, active=?, draftId=?, ctaUrl=?, ctaLabel=?, trackLinks=?, linkId=?, buttons=? WHERE id=?`)
  .run(
   normalizedCompanyId,
   bodyText,
   platform,
   startDate,
   endDate,
   activeValue,
   normalizedDraftId,
   normalizedCtaUrl,
   normalizeText(ctaLabel),
   trackLinksValue,
   linkId,
   serializedButtons,
   id
  );
 invalidateDerivedCaches();
 res.send({ ok: true });
});

app.delete('/posts/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid post id');
 const removePost = db.transaction((postId) => {
  db.prepare('DELETE FROM schedule_items WHERE postId = ?').run(postId);
  db.prepare('DELETE FROM posts WHERE id = ?').run(postId);
 });
 removePost(id);
 invalidateDerivedCaches();
 res.send({ ok: true });
});

app.post('/posts/:id/publish', async (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid post id');
 try {
  const result = await publishPostNow(id);
  if (result.ok) return res.send(result);
  return res.status(500).send(result);
 } catch (e) {
  res.status(500).send({ ok: false, error: e.message });
 }
});

app.post('/posts/:id/renew', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid post id');
 const post = db.prepare('SELECT id, startDate, endDate FROM posts WHERE id = ?').get(id);
 if (!post) return res.status(404).send({ ok: false, error: 'Post not found' });
 const today = getCurrentDateString();
 const base = maxDateString(post.endDate, post.startDate, today) || today;
 const newEndDate = addDaysToDateString(base, 30);
 if (!newEndDate) return res.status(500).send({ ok: false, error: 'Renew failed' });
 db.prepare('UPDATE posts SET endDate=?, active=1 WHERE id=?').run(newEndDate, id);
 invalidateDerivedCaches();
 res.send({ ok: true, endDate: newEndDate });
});

app.post('/schedule/run', async (req, res) => {
 const date = req.body?.date;
 if (date && !isValidDateString(date)) return badRequest(res, 'Invalid date format');
 const startFromNow = !(req.body?.startFromNow === 0 || req.body?.startFromNow === false || req.body?.startFromNow === '0');
 try {
  const forceImmediateSession = req.body?.forceImmediateSession === 1
   || req.body?.forceImmediateSession === true
   || req.body?.forceImmediateSession === '1';
  const result = generateDailySchedule(date, startFromNow, { forceImmediateSession });
  processDueSchedule(true).catch((e) => console.log('Manual schedule send failed:', e.message));
  res.send({ ok: true, result });
 } catch (e) {
  res.status(500).send({ ok: false, error: e.message });
 }
});

app.get('/schedule/forecast', (req, res) => {
 const date = req.query?.date;
 if (date && !isValidDateString(date)) return badRequest(res, 'Invalid date format');
 const targetDate = date || getCurrentDateString();
 const settings = getSchedulerSettings();

 const startTimeRaw = req.query?.startTime;
 const secondStartTimeRaw = req.query?.secondStartTime;
 const minIntervalRaw = req.query?.minIntervalMinutes;
 const startFromNowRaw = req.query?.startFromNow;
 const forceImmediateRaw = req.query?.forceImmediateSession;

 const hasStartTime = hasNonEmptyValue(startTimeRaw);
 const hasSecondStartTimeInput = secondStartTimeRaw !== undefined && secondStartTimeRaw !== null;
 const hasSecondStartTime = hasNonEmptyValue(secondStartTimeRaw);
 const hasMinInterval = hasNonEmptyValue(minIntervalRaw);
 const startFromNow = startFromNowRaw === '1' || startFromNowRaw === 1 || startFromNowRaw === true || startFromNowRaw === 'true';
 const forceImmediateSession = forceImmediateRaw === '1'
  || forceImmediateRaw === 1
  || forceImmediateRaw === true
  || forceImmediateRaw === 'true';

 const scheduleTime = hasStartTime ? normalizeTime(startTimeRaw) : settings.scheduleTime;
 let secondScheduleTime = settings.secondScheduleTime;
 if (hasSecondStartTimeInput) {
  secondScheduleTime = hasSecondStartTime ? normalizeTime(secondStartTimeRaw) : null;
 }
 const minIntervalMinutes = hasMinInterval ? Number(minIntervalRaw) : settings.minIntervalMinutes;

 if (!scheduleTime) return badRequest(res, 'Invalid first start time');
 if (hasSecondStartTimeInput && hasSecondStartTime && !secondScheduleTime) {
  return badRequest(res, 'Invalid second start time');
 }
 if (secondScheduleTime && scheduleTime === secondScheduleTime) {
  return badRequest(res, 'First and second start time must be different');
 }
 if (!Number.isFinite(minIntervalMinutes) || minIntervalMinutes < 1 || minIntervalMinutes > 1440) {
  return badRequest(res, 'Invalid interval minutes');
 }

 const forecastConfig = {
  scheduleTime,
  secondScheduleTime,
  minIntervalMinutes: Math.floor(minIntervalMinutes)
 };
 res.send(getCachedScheduleForecast(targetDate, forecastConfig, {
  startFromNow,
  forceImmediateSession
 }));
});

app.get('/schedule/items', (req, res) => {
 const date = req.query?.date;
 if (date && !isValidDateString(date)) return badRequest(res, 'Invalid date format');
 const targetDate = date || getCurrentDateString();
 const rows = db.prepare(`
  SELECT schedule_items.*,
   posts.platform as postPlatform,
   posts.text as postText,
   posts.ctaLabel as ctaLabel,
   posts.ctaUrl as ctaUrl,
   posts.buttons as buttons,
   posts.draftId as draftId,
   drafts.mediaType as draftMediaType,
   COALESCE(companies.premium, 0) as companyPremium,
   companies.name as companyName
 FROM schedule_items
 JOIN posts ON schedule_items.postId = posts.id
 JOIN companies ON posts.companyId = companies.id
 LEFT JOIN drafts ON posts.draftId = drafts.id
 WHERE schedule_items.scheduledAt >= ? AND schedule_items.scheduledAt < ? AND posts.platform = 'telegram'
 ORDER BY schedule_items.scheduledAt ASC
`).all(`${targetDate}T00:00:00`, `${targetDate}T23:59:59.999`);
 res.send(rows);
});

app.get('/drafts', (_, res) => {
 const rows = db.prepare(`
  SELECT * FROM drafts ORDER BY createdAt DESC, id DESC
 `).all();
 res.send(rows.map((row) => ({
  ...row,
  buttons: parseStoredButtons(row.buttons)
 })));
});

app.delete('/drafts/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid draft id');
 db.prepare('DELETE FROM drafts WHERE id = ?').run(id);
 res.send({ ok: true });
});

app.post('/telegram/webhook', async (req, res) => {
 const secret = process.env.TELEGRAM_WEBHOOK_SECRET;
 if (secret) {
  const header = req.headers['x-telegram-bot-api-secret-token'];
  if (header !== secret) return res.status(401).send({ ok: false });
 }
 const update = req.body;
 const message = update?.channel_post || update?.message;
 if (message) {
  await handleTelegramMessage(message);
 }
 res.send({ ok: true });
});

app.post('/telegram/pull', async (req, res) => {
 try {
  const count = await pullTelegramUpdates();
  res.send({ ok: true, processed: count });
 } catch (e) {
  res.status(500).send({ ok: false, error: e.message });
 }
});

app.get('/analytics/companies', (_, res) => {
 try {
  res.send(getCachedCompanyAnalyticsReport());
 } catch (e) {
  res.status(500).send({ ok: false, error: e.message });
 }
});

app.get('/logs/query', (req, res) => {
 const page = parsePositiveInt(req.query?.page, 1, 1, 1000000);
 const pageSize = parsePositiveInt(req.query?.pageSize, DEFAULT_LOGS_PAGE_SIZE, 1, MAX_LOGS_PAGE_SIZE);
 const filters = buildLogsQueryParts({
  status: req.query?.status,
  query: req.query?.q
 });

 const totalRow = db.prepare(`
  SELECT COUNT(*) as total
  FROM logs
  LEFT JOIN posts ON logs.postId = posts.id
  LEFT JOIN drafts ON posts.draftId = drafts.id
  LEFT JOIN companies ON COALESCE(logs.companyId, posts.companyId) = companies.id
  ${filters.whereSql}
 `).get(...filters.params);
 const total = Number(totalRow?.total || 0);
 const totalPages = Math.max(1, Math.ceil(total / pageSize));
 const normalizedPage = Math.min(Math.max(1, page), totalPages);
 const offset = (normalizedPage - 1) * pageSize;

 const items = db.prepare(`
  SELECT logs.*,
   posts.text as postText,
   posts.draftId as draftId,
   posts.platform as postPlatform,
   drafts.mediaType as draftMediaType,
   drafts.caption as draftCaption,
   drafts.text as draftText,
   COALESCE(logs.companyName, companies.name) as companyName
  FROM logs
  LEFT JOIN posts ON logs.postId = posts.id
  LEFT JOIN drafts ON posts.draftId = drafts.id
  LEFT JOIN companies ON COALESCE(logs.companyId, posts.companyId) = companies.id
  ${filters.whereSql}
  ORDER BY logs.id DESC
  LIMIT ? OFFSET ?
 `).all(...filters.params, pageSize, offset);

 const statusRows = db.prepare(`
  SELECT status, COUNT(*) as total
  FROM logs
  WHERE platform = 'telegram'
  GROUP BY status
 `).all();
 const statusCounts = { sent: 0, failed: 0, pending: 0, other: 0, total: 0 };
 for (const row of statusRows) {
  const key = String(row?.status || '').toLowerCase();
  const value = Number(row?.total || 0);
  if (key === 'sent' || key === 'failed' || key === 'pending') {
   statusCounts[key] = value;
  } else {
   statusCounts.other += value;
  }
  statusCounts.total += value;
 }
 if (!statusCounts.other) {
  delete statusCounts.other;
 }

 const today = getCurrentDateString();
 const todayRow = db.prepare(`
  SELECT COUNT(*) as total
  FROM logs
  WHERE platform = 'telegram'
   AND (date = ? OR (date IS NULL AND substr(createdAt, 1, 10) = ?))
 `).get(today, today);

 const from = total ? offset + 1 : 0;
 const to = total ? Math.min(offset + items.length, total) : 0;
 res.send({
  items,
  page: normalizedPage,
  pageSize,
  total,
  totalPages,
  from,
  to,
  statusCounts,
  todayCount: Number(todayRow?.total || 0),
  filters: {
   status: filters.status || '',
   q: filters.queryText
  }
 });
});

// Backward-compatible full logs endpoint kept for older clients.
app.get('/logs', (_, res) => res.send(db.prepare(`
 SELECT logs.*,
  posts.text as postText,
  posts.draftId as draftId,
  posts.platform as postPlatform,
  drafts.mediaType as draftMediaType,
  drafts.caption as draftCaption,
  drafts.text as draftText,
  COALESCE(logs.companyName, companies.name) as companyName
 FROM logs
 LEFT JOIN posts ON logs.postId = posts.id
 LEFT JOIN drafts ON posts.draftId = drafts.id
 LEFT JOIN companies ON COALESCE(logs.companyId, posts.companyId) = companies.id
 WHERE logs.platform = 'telegram'
 ORDER BY logs.id DESC
`).all()));

let runtimeTickHandle = null;
let httpServer = null;

function startRuntimeWorkers() {
 if (runtimeTickHandle) return;
 configureScheduleJob();
 runtimeTickHandle = setInterval(() => {
  processDueSchedule().catch((e) => console.log('Schedule tick failed:', e.message));
 }, 30000);
}

function stopRuntimeWorkers() {
 if (runtimeTickHandle) {
  clearInterval(runtimeTickHandle);
  runtimeTickHandle = null;
 }
 if (scheduleJobs.length) {
  for (const job of scheduleJobs) {
   try {
    job.stop();
   } catch (e) {}
  }
  scheduleJobs = [];
 }
}

function startHttpServer(port = PORT) {
 if (httpServer) return httpServer;
 httpServer = app.listen(port, () => console.log(`Back is up on ${port}!`));
 return httpServer;
}

function stopHttpServer() {
 if (!httpServer) return;
 httpServer.close();
 httpServer = null;
}

if (!RUNTIME_DISABLED) {
 startRuntimeWorkers();
 startHttpServer(PORT);
}

export {
 app,
 db,
 getSetting,
 setSetting,
 getCurrentDateString,
 nowLocalIso,
 buildRuntimePlan,
 generateDailySchedule,
 cleanupOrphanScheduleItems,
 markExpiredPendingScheduleItems,
 markInvalidPendingScheduleItems,
 reclaimStaleProcessingItems,
 claimNextDueScheduleItem,
 processDueSchedule,
 startRuntimeWorkers,
 stopRuntimeWorkers,
 startHttpServer,
 stopHttpServer
};
