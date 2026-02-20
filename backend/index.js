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
const REGULAR_ROTATION_WEIGHT = 2;
const PREMIUM_ROTATION_WEIGHT = 3;
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

function getSchedulerDefaults() {
 const fromSetting = normalizeTime(getSetting('scheduleTime'));
 if (fromSetting) return fromSetting;
 const cronMatch = String(CRON_TIME || '').match(/^(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\*$/);
 if (cronMatch) {
  const mm = String(cronMatch[1]).padStart(2, '0');
  const hh = String(cronMatch[2]).padStart(2, '0');
  return `${hh}:${mm}`;
 }
 return '09:00';
}

function getSchedulerSettings() {
 const scheduleTime = normalizeTime(getSetting('scheduleTime')) || getSchedulerDefaults();
 const runtimeEndTime = normalizeTime(getSetting('runtimeEndTime')) || '23:00';
 const minIntervalMinutes = Number(getSetting('minIntervalMinutes') || 20);
 const rotationGapMinutes = Number(getSetting('rotationGapMinutes') || 0);
 const enabledRaw = getSetting('scheduleEnabled');
 const enabled = enabledRaw === null ? true : enabledRaw !== '0';
 return {
  scheduleTime,
  runtimeEndTime,
  minIntervalMinutes: Number.isFinite(minIntervalMinutes) && minIntervalMinutes > 0 ? Math.floor(minIntervalMinutes) : 20,
  rotationGapMinutes: Number.isFinite(rotationGapMinutes) && rotationGapMinutes >= 0 ? Math.floor(rotationGapMinutes) : 0,
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
 const explicitCursor = Number(options.startCursor);
 const cursorToken = Number.isFinite(explicitCursor)
  ? `explicit:${Math.floor(explicitCursor)}`
  : `stored:${String(getSetting('runtimeRotationCursor') || '0')}`;
 const key = [
  scheduleForecastVersion,
  targetDate,
  config.scheduleTime,
  config.runtimeEndTime,
  Number(config.minIntervalMinutes) || 0,
  Number(config.rotationGapMinutes) || 0,
  startFromNow ? 1 : 0,
  cursorToken
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
 const startCursor = Number.isFinite(explicitCursor)
  ? Math.floor(explicitCursor)
  : getStoredRotationCursor(posts.length);
 const payload = calculateScheduleForecast(targetDate, posts, config, {
  ...options,
  startFromNow,
  startCursor
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

function getPostCompanyKey(post) {
 const companyId = Number(post?.companyId || 0);
 if (companyId) return `id:${companyId}`;
 const companyName = normalizeText(post?.companyName);
 if (companyName) return `name:${companyName.toLowerCase()}`;
 const postId = Number(post?.id || 0);
 if (postId) return `post:${postId}`;
 return null;
}

function getPostRotationWeight(post) {
 return Number(post?.companyPremium) === 1 ? PREMIUM_ROTATION_WEIGHT : REGULAR_ROTATION_WEIGHT;
}

function normalizeRotationCursor(value, sequenceLength) {
 const raw = Number(value || 0);
 if (!sequenceLength || !Number.isFinite(raw) || raw < 0) return 0;
 return Math.floor(raw) % sequenceLength;
}

function getStoredRotationCursor(sequenceLength) {
 return normalizeRotationCursor(getSetting('runtimeRotationCursor'), sequenceLength);
}

function storeRotationCursor(cursor, targetDate, sequenceLength) {
 if (!sequenceLength) return;
 setSetting('runtimeRotationCursor', String(normalizeRotationCursor(cursor, sequenceLength)));
 if (isValidDateString(targetDate)) {
  setSetting('runtimeRotationDate', targetDate);
 }
}

function buildWeightedRotation(posts) {
 const sortedPosts = [...posts].sort(compareCompanyPosts);
 if (!sortedPosts.length) {
  return { sequence: [], totalWeight: 0, sortedPosts };
 }
 const nodes = sortedPosts.map((post, index) => ({
  post,
  index,
  weight: getPostRotationWeight(post),
  current: 0
 }));
 const totalWeight = nodes.reduce((sum, node) => sum + node.weight, 0);
 if (!totalWeight) {
  return { sequence: [], totalWeight: 0, sortedPosts };
 }
 const sequence = [];
 for (let step = 0; step < totalWeight; step += 1) {
  let best = null;
  for (const node of nodes) {
   node.current += node.weight;
   if (!best || node.current > best.current) {
    best = node;
    continue;
   }
   if (node.current === best.current) {
    const byPost = compareCompanyPosts(node.post, best.post);
    if (byPost < 0 || (byPost === 0 && node.index < best.index)) {
     best = node;
    }
   }
  }
  best.current -= totalWeight;
  sequence.push(best.post);
 }
 return { sequence, totalWeight, sortedPosts };
}

function getCompanySentCountMap(companyIds) {
 const uniqueIds = Array.from(new Set((companyIds || [])
  .map((id) => Number(id || 0))
  .filter((id) => id > 0)));
 if (!uniqueIds.length) return new Map();
 const placeholders = uniqueIds.map(() => '?').join(',');
 const rows = db.prepare(`
  SELECT companyId, COUNT(*) as sentCount
  FROM logs
  WHERE platform = 'telegram'
   AND status = 'sent'
   AND companyId IN (${placeholders})
  GROUP BY companyId
 `).all(...uniqueIds);
 const map = new Map();
 for (const row of rows) {
  const companyId = Number(row?.companyId || 0);
  if (!companyId) continue;
  map.set(companyId, Number(row?.sentCount || 0));
 }
 return map;
}

function getCyclicDistance(index, pointer, size) {
 if (!size) return 0;
 return (index - pointer + size) % size;
}

function buildRuntimePlan(targetDate, posts, config, options = {}) {
 const minIntervalMinutes = Math.max(1, Number(config.minIntervalMinutes) || 1);
 const rotationGapMinutes = Math.max(0, Number(config.rotationGapMinutes) || 0);
 const windowStart = timeToMinutes(config.scheduleTime);
 const windowEnd = timeToMinutes(config.runtimeEndTime);
 const sortedPosts = [...posts].sort(compareCompanyPosts);
 const perPostCounts = new Map(sortedPosts.map((post) => [post.id, 0]));
 const preferredMinuteByPost = new Map();
 const pendingPreferred = [];
 const companyMap = new Map();
 for (const post of sortedPosts) {
  const companyKey = getPostCompanyKey(post) || `post:${post.id}`;
  if (!companyMap.has(companyKey)) {
   companyMap.set(companyKey, {
    key: companyKey,
    companyId: Number(post?.companyId || 0) || null,
    companyName: normalizeText(post?.companyName) || null,
    companyPremium: Number(post?.companyPremium) === 1 ? 1 : 0,
    weight: getPostRotationWeight(post),
    posts: [],
    sentCount: 0,
    plannedCount: 0,
    postCursor: 0,
    orderIndex: 0
   });
  }
  const company = companyMap.get(companyKey);
  company.posts.push(post);
  if (Number(post?.companyPremium) === 1) {
   company.companyPremium = 1;
   company.weight = PREMIUM_ROTATION_WEIGHT;
  }
  const preferredMinute = timeToMinutes(post?.preferredTime);
  if (preferredMinute === null) continue;
  preferredMinuteByPost.set(post.id, preferredMinute);
  pendingPreferred.push({ post, preferredMinute, companyKey });
 }
 const companies = Array.from(companyMap.values()).sort((a, b) => {
  const nameA = String(a?.companyName || '');
  const nameB = String(b?.companyName || '');
  const byName = nameA.localeCompare(nameB, 'en', { sensitivity: 'base' });
  if (byName) return byName;
  return String(a?.key || '').localeCompare(String(b?.key || ''), 'en', { sensitivity: 'base' });
 });
 for (let i = 0; i < companies.length; i += 1) {
  companies[i].orderIndex = i;
  companies[i].posts.sort(compareCompanyPosts);
 }
 const companyOrderIndex = new Map(companies.map((company, index) => [company.key, index]));
 const sentCountByCompanyId = getCompanySentCountMap(companies.map((company) => company.companyId));
 for (const company of companies) {
  if (!company.companyId) continue;
  company.sentCount = sentCountByCompanyId.get(company.companyId) || 0;
 }
 const sequenceLength = companies.length;
 const totalWeight = companies.reduce((sum, company) => sum + Math.max(1, Number(company.weight) || 1), 0);
 pendingPreferred.sort((a, b) => {
  if (a.preferredMinute !== b.preferredMinute) return a.preferredMinute - b.preferredMinute;
  return compareCompanyPosts(a.post, b.post);
 });
 const defaultResult = {
  sequenceLength,
  totalWeight,
  sortedPosts,
  perPostCounts,
  slots: [],
  totalPublications: 0,
  fullRotations: 0,
  partialPublications: 0,
  startCursor: 0,
  endCursor: 0,
  windowStartMinutes: windowStart,
  windowEndMinutes: windowEnd
 };
 if (!sequenceLength || windowStart === null || windowEnd === null || windowEnd < windowStart) {
  return defaultResult;
 }

 let runtimeStartMinutes = windowStart;
 const isToday = targetDate === getCurrentDateString();
 if (options.startFromNow && isToday) {
  const nowMinutes = getCurrentMinutes();
  runtimeStartMinutes = Math.max(runtimeStartMinutes, nowMinutes);
 }
 if (runtimeStartMinutes > windowEnd) {
  return {
   ...defaultResult,
   startCursor: normalizeRotationCursor(options.startCursor, sequenceLength),
   endCursor: normalizeRotationCursor(options.startCursor, sequenceLength),
   windowStartMinutes: runtimeStartMinutes
  };
 }

 const startCursor = options.startCursor === undefined || options.startCursor === null
  ? getStoredRotationCursor(sequenceLength)
  : normalizeRotationCursor(options.startCursor, sequenceLength);
 let pointer = startCursor;
 let currentMinutes = runtimeStartMinutes;
 const slots = [];
 const preferredReleased = new Set();

 const isRotatingPostBlockedByPreferredWindow = (candidate) => {
  const preferredMinute = preferredMinuteByPost.get(candidate.id);
  const waitingPreferredWindow = preferredMinute !== undefined &&
   !preferredReleased.has(candidate.id) &&
   currentMinutes < preferredMinute;
  return waitingPreferredWindow;
 };

 const hasAvailableRotatingPostInCompany = (company) => {
  return company.posts.some((post) => !isRotatingPostBlockedByPreferredWindow(post));
 };

 const pickPostFromCompany = (company, lastPostId = null) => {
  const totalPosts = company.posts.length;
  if (!totalPosts) return null;
  let fallback = null;
  for (let shift = 0; shift < totalPosts; shift += 1) {
   const index = (company.postCursor + shift) % totalPosts;
   const candidate = company.posts[index];
   if (isRotatingPostBlockedByPreferredWindow(candidate)) continue;
   const nextCursor = (index + 1) % totalPosts;
   if (!fallback) fallback = { post: candidate, nextCursor };
   if (lastPostId && Number(candidate?.id || 0) === Number(lastPostId) && totalPosts > 1) {
    continue;
   }
   return { post: candidate, nextCursor };
  }
  return fallback;
 };

 const pickBestCompanyFromKeys = (candidateKeys, lastCompanyKey = null, requireDifferentCompany = false) => {
  const uniqueKeys = Array.from(new Set((candidateKeys || []).filter((key) => companyMap.has(key))));
  if (!uniqueKeys.length) return null;
  let keys = uniqueKeys;
  if (requireDifferentCompany && lastCompanyKey) {
   const filtered = keys.filter((key) => key !== lastCompanyKey);
   if (!filtered.length) return null;
   keys = filtered;
  }
  let best = null;
  for (const key of keys) {
   const company = companyMap.get(key);
   if (!company) continue;
   const weight = Math.max(1, Number(company.weight) || 1);
   const score = (Number(company.sentCount || 0) + Number(company.plannedCount || 0)) / weight;
   const orderIndex = companyOrderIndex.get(company.key) ?? 0;
   const distance = getCyclicDistance(orderIndex, pointer, sequenceLength);
   if (!best) {
    best = { company, score, distance };
    continue;
   }
   if (score < best.score - 1e-9) {
    best = { company, score, distance };
    continue;
   }
   if (Math.abs(score - best.score) <= 1e-9) {
    if (distance < best.distance) {
     best = { company, score, distance };
     continue;
    }
    if (distance === best.distance) {
     const byName = String(company.companyName || '').localeCompare(String(best.company.companyName || ''), 'en', { sensitivity: 'base' });
     if (byName < 0) {
      best = { company, score, distance };
      continue;
     }
     if (byName === 0) {
      const byKey = String(company.key || '').localeCompare(String(best.company.key || ''), 'en', { sensitivity: 'base' });
      if (byKey < 0) {
       best = { company, score, distance };
      }
     }
    }
   }
  }
  return best?.company || null;
 };

 const getDuePreferredEntries = () => {
  const due = [];
  for (let i = 0; i < pendingPreferred.length; i += 1) {
   const entry = pendingPreferred[i];
   if (entry.preferredMinute > currentMinutes) break;
   due.push({ index: i, entry });
  }
  return due;
 };

 const pickDuePreferredPostForCompany = (preferredCompany, duePreferredRows) => {
  if (!preferredCompany) return null;
  const dueRow = duePreferredRows.find((row) => row.entry.companyKey === preferredCompany.key);
  if (!dueRow) return null;
  const selectedPost = dueRow.entry.post;
  pendingPreferred.splice(dueRow.index, 1);
  pointer = (preferredCompany.orderIndex + 1) % sequenceLength;
  const preferredPostIndex = preferredCompany.posts.findIndex((item) => Number(item?.id || 0) === Number(selectedPost?.id || 0));
  if (preferredPostIndex >= 0) {
   preferredCompany.postCursor = (preferredPostIndex + 1) % preferredCompany.posts.length;
  }
  return selectedPost;
 };

 const getRotatingCompanyKeys = () => {
  return companies
   .filter((company) => hasAvailableRotatingPostInCompany(company))
   .map((company) => company.key);
 };

 while (currentMinutes <= windowEnd) {
  let post = null;
  let usedPreferredOverride = false;
  const previousSlot = slots[slots.length - 1];
  const lastCompanyKey = getPostCompanyKey(previousSlot?.post);
  const lastPostId = Number(previousSlot?.post?.id || 0) || null;

  const duePreferred = getDuePreferredEntries();
  const duePreferredCompanyKeys = Array.from(new Set(duePreferred.map((row) => row.entry.companyKey)));
  const rotatingCompanyKeys = getRotatingCompanyKeys();

  const preferredDifferentCompany = pickBestCompanyFromKeys(duePreferredCompanyKeys, lastCompanyKey, Boolean(lastCompanyKey));
  post = pickDuePreferredPostForCompany(preferredDifferentCompany, duePreferred);
  if (post) {
   usedPreferredOverride = true;
  }

  if (!post) {
   const rotatingDifferentCompany = pickBestCompanyFromKeys(rotatingCompanyKeys, lastCompanyKey, Boolean(lastCompanyKey));
   if (rotatingDifferentCompany) {
    const pickedPost = pickPostFromCompany(rotatingDifferentCompany, lastPostId);
    if (pickedPost) {
     post = pickedPost.post;
     rotatingDifferentCompany.postCursor = pickedPost.nextCursor;
     pointer = (rotatingDifferentCompany.orderIndex + 1) % sequenceLength;
    }
   }
  }

  if (!post) {
   const preferredAnyCompany = pickBestCompanyFromKeys(duePreferredCompanyKeys);
   post = pickDuePreferredPostForCompany(preferredAnyCompany, duePreferred);
   if (post) {
    usedPreferredOverride = true;
   }
  }

  if (!post) {
   const rotatingAnyCompany = pickBestCompanyFromKeys(rotatingCompanyKeys);
   if (rotatingAnyCompany) {
    const pickedPost = pickPostFromCompany(rotatingAnyCompany, lastPostId);
    if (pickedPost) {
     post = pickedPost.post;
     rotatingAnyCompany.postCursor = pickedPost.nextCursor;
     pointer = (rotatingAnyCompany.orderIndex + 1) % sequenceLength;
    }
   }
   if (!post && !rotatingCompanyKeys.length && pendingPreferred.length) {
    const nextPreferredMinute = pendingPreferred[0].preferredMinute;
    if (nextPreferredMinute > currentMinutes) {
     currentMinutes = nextPreferredMinute;
     continue;
    }
   }
  }

  if (!post) break;
  if (preferredMinuteByPost.has(post.id)) {
   preferredReleased.add(post.id);
  }
  const selectedCompanyKey = getPostCompanyKey(post);
  const selectedCompany = selectedCompanyKey ? companyMap.get(selectedCompanyKey) : null;
  if (selectedCompany) {
   selectedCompany.plannedCount += 1;
  }
  slots.push({
   post,
   minutes: currentMinutes,
   source: usedPreferredOverride ? 'preferred' : 'rotation',
   rotationCursorAfter: pointer
  });
  perPostCounts.set(post.id, (perPostCounts.get(post.id) || 0) + 1);
  currentMinutes += minIntervalMinutes;
  if (!usedPreferredOverride && pointer === 0) currentMinutes += rotationGapMinutes;
 }

 const totalPublications = slots.length;
 const fullRotations = sequenceLength ? Math.floor(totalPublications / sequenceLength) : 0;
 const partialPublications = sequenceLength ? totalPublications % sequenceLength : 0;

 return {
  sequenceLength,
  totalWeight,
  sortedPosts,
  perPostCounts,
  slots,
  totalPublications,
  fullRotations,
  partialPublications,
  startCursor,
  endCursor: pointer,
  windowStartMinutes: runtimeStartMinutes,
  windowEndMinutes: windowEnd
 };
}

function calculateScheduleForecast(date, posts, config, options = {}) {
 const plan = buildRuntimePlan(date, posts, config, {
  startFromNow: Boolean(options.startFromNow),
  startCursor: options.startCursor
 });
 const perPost = plan.sortedPosts.map((post) => ({
  postId: post.id,
  companyName: post.companyName || null,
  companyPremium: Number(post.companyPremium) === 1 ? 1 : 0,
  weight: getPostRotationWeight(post),
  publishCount: plan.perPostCounts.get(post.id) || 0
 }));

 const distributionMap = new Map();
 for (const row of perPost) {
  distributionMap.set(row.publishCount, (distributionMap.get(row.publishCount) || 0) + 1);
 }
 const publishDistribution = Array.from(distributionMap.entries())
  .map(([publishCount, postCount]) => ({ publishCount, postCount }))
  .sort((a, b) => b.publishCount - a.publishCount);

 let equalRotations = true;
 if (perPost.length) {
  const normalizedShares = perPost.map((row) => row.publishCount / Math.max(1, row.weight));
  const maxShare = Math.max(...normalizedShares);
  const minShare = Math.min(...normalizedShares);
  equalRotations = (maxShare - minShare) <= 1;
 }

 const suggestions = perPost.length ? {
  overall: {
   scheduleTime: config.scheduleTime,
   runtimeEndTime: config.runtimeEndTime,
   minIntervalMinutes: config.minIntervalMinutes,
   rotationGapMinutes: config.rotationGapMinutes,
   expectedPublications: plan.totalPublications,
   expectedFullRotations: plan.fullRotations
  }
 } : {};

 return {
  date,
  config: {
   scheduleTime: config.scheduleTime,
   runtimeEndTime: config.runtimeEndTime,
   minIntervalMinutes: config.minIntervalMinutes,
   rotationGapMinutes: config.rotationGapMinutes
  },
  totals: {
   windowStartTime: minutesToTimeString(plan.windowStartMinutes) || config.scheduleTime,
   windowEndTime: minutesToTimeString(plan.windowEndMinutes) || config.runtimeEndTime,
   activePosts: perPost.length,
   totalWeight: plan.totalWeight,
   totalPublications: plan.totalPublications,
   fullRotations: plan.fullRotations,
   partialPublications: plan.partialPublications,
   equalRotations,
   rotationCursorStart: plan.startCursor,
   rotationCursorEnd: plan.endCursor
  },
  perPost,
  publishDistribution,
  suggestions
 };
}

function generateDailySchedule(date, startFromNow = true, options = {}) {
 const targetDate = date || getCurrentDateString();
 const settings = getSchedulerSettings();
 const posts = getActiveTelegramPostsForDate(targetDate);

 if (!posts.length) return { date: targetDate, total: 0, scheduled: 0 };

 const dayStart = `${targetDate}T00:00:00`;
 const dayEnd = `${targetDate}T23:59:59.999`;

 const plan = buildRuntimePlan(targetDate, posts, settings, {
  startFromNow,
  startCursor: options.startCursor
 });
 const createdAt = new Date().toISOString();
 const persistSchedule = db.transaction(() => {
  db.prepare(`DELETE FROM schedule_items WHERE scheduledAt >= ? AND scheduledAt <= ? AND status = 'pending'`)
   .run(dayStart, dayEnd);
  for (const slot of plan.slots) {
   const scheduledAt = minutesToIso(targetDate, slot.minutes);
   if (!scheduledAt) continue;
   db.prepare(`INSERT INTO schedule_items (postId, scheduledAt, status, createdAt, rotationCursorAfter) VALUES (?,?,?,?,?)`)
    .run(slot.post.id, scheduledAt, 'pending', createdAt, Number(slot.rotationCursorAfter) || 0);
  }
 });
 persistSchedule();

 return {
  date: targetDate,
  total: posts.length,
  scheduled: plan.totalPublications,
  rotationSize: plan.sequenceLength,
  startCursor: plan.startCursor,
  endCursor: plan.endCursor,
  windowStartTime: minutesToTimeString(plan.windowStartMinutes),
  windowEndTime: minutesToTimeString(plan.windowEndMinutes)
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

function storeRotationCursorFromScheduleItem(item) {
 const cursor = Number(item?.rotationCursorAfter);
 if (!Number.isFinite(cursor) || cursor < 0) return;
 setSetting('runtimeRotationCursor', String(Math.floor(cursor)));
 const date = toDatePart(item?.scheduledAt);
 if (date) {
  setSetting('runtimeRotationDate', date);
 }
 invalidateDerivedCaches({ analytics: false });
}

let scheduleProcessing = false;
async function processDueSchedule(force = false) {
 if (scheduleProcessing) return;
 scheduleProcessing = true;
 try {
  const settings = getSchedulerSettings();
  if (!force && !settings.enabled) return;
  const minIntervalMs = Math.max(1, settings.minIntervalMinutes) * 60 * 1000;
  const lastSentAt = getSetting('lastSentAt');
  if (!force && lastSentAt) {
   const delta = Date.now() - new Date(lastSentAt).getTime();
   if (delta < minIntervalMs) return;
  }

  cleanupOrphanScheduleItems();
  reclaimStaleProcessingItems(Math.max(10, settings.minIntervalMinutes * 2));
  const currentDate = getCurrentDateString();
  markExpiredPendingScheduleItems(currentDate);
  markInvalidPendingScheduleItems();
  const nowIso = nowLocalIso();
  const claimedScheduleId = claimNextDueScheduleItem(nowIso, currentDate);
  if (!claimedScheduleId) return;
  const item = db.prepare(`
   SELECT
    schedule_items.id as scheduleId,
    schedule_items.postId as schedulePostId,
    schedule_items.scheduledAt,
    schedule_items.status,
    schedule_items.error,
    schedule_items.createdAt,
    schedule_items.sentAt,
    schedule_items.rotationCursorAfter,
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

  if (!item) return;

  const sentAt = new Date().toISOString();
  if (!item.platform) {
   db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
    .run('failed', sentAt, 'Post not found', null, item.scheduleId);
   return;
  }
  if (item.platform !== 'telegram') {
   db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
    .run('failed', sentAt, 'Unsupported platform', null, item.scheduleId);
   return;
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
   return;
  }
  try {
   let deliveryMeta = null;
   deliveryMeta = await sendTelegramPost(item);
   db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
    .run('sent', sentAt, null, null, item.scheduleId);
   insertLog(item, 'sent', null, 'auto', null, deliveryMeta);
   storeRotationCursorFromScheduleItem(item);
   setSetting('lastSentAt', sentAt);
  } catch (e) {
   db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=?, processingStartedAt=? WHERE id=?')
    .run('failed', sentAt, e.message, null, item.scheduleId);
   insertLog(item, 'failed', e.message, 'auto');
  }
 } finally {
  scheduleProcessing = false;
 }
}

let scheduleJob = null;
function configureScheduleJob() {
 if (scheduleJob) {
  scheduleJob.stop();
  scheduleJob = null;
 }
 const settings = getSchedulerSettings();
 if (!settings.enabled) return;
 const scheduleTime = normalizeTime(settings.scheduleTime);
 if (!scheduleTime) return;
 const [hh, mm] = scheduleTime.split(':').map(Number);
 const cronExpr = `${mm} ${hh} * * *`;
 scheduleJob = cron.schedule(cronExpr, async () => {
  generateDailySchedule(undefined, true);
 }, CRON_TZ ? { timezone: CRON_TZ } : undefined);
}

function ensureTodaySchedule() {
 const settings = getSchedulerSettings();
 if (!settings.enabled) return;
 const scheduleTime = normalizeTime(settings.scheduleTime);
 if (!scheduleTime) return;
 const today = getCurrentDateString();
 const dayStart = `${today}T00:00:00`;
 const dayEnd = `${today}T23:59:59.999`;
 const existing = db.prepare('SELECT COUNT(*) as count FROM schedule_items WHERE scheduledAt >= ? AND scheduledAt <= ?')
  .get(dayStart, dayEnd)?.count || 0;
 if (existing > 0) return;
 const [hh, mm] = scheduleTime.split(':').map(Number);
 const scheduleMinutes = hh * 60 + mm;
 const nowMinutes = getCurrentMinutes();
 if (nowMinutes >= scheduleMinutes) {
  generateDailySchedule(today, true);
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
 rotationCursorAfter INTEGER
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
ensureColumn('schedule_items', 'rotationCursorAfter', 'INTEGER');
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
 res.send({
  scheduleEnabled: settings.enabled ? 1 : 0,
  scheduleTime: settings.scheduleTime,
  runtimeEndTime: settings.runtimeEndTime,
  minIntervalMinutes: settings.minIntervalMinutes,
  rotationGapMinutes: settings.rotationGapMinutes
 });
});

app.put('/settings', (req, res) => {
 const currentSettings = getSchedulerSettings();
 const scheduleEnabled = req.body?.scheduleEnabled;
 const runNow = req.body?.runNow === 1 || req.body?.runNow === true || req.body?.runNow === '1';
 const runDateInput = String(req.body?.runDate || '').trim();
 const runDate = runDateInput || getCurrentDateString();
 if (runNow && !isValidDateString(runDate)) return badRequest(res, 'Invalid run date');
 const scheduleTime = normalizeTime(req.body?.scheduleTime);
 const runtimeEndTimeInput = req.body?.runtimeEndTime;
 const runtimeEndTime = hasNonEmptyValue(runtimeEndTimeInput)
  ? normalizeTime(runtimeEndTimeInput)
  : currentSettings.runtimeEndTime;
 const minIntervalMinutesRaw = Number(req.body?.minIntervalMinutes);
 const rotationGapInput = req.body?.rotationGapMinutes;
 const rotationGapMinutesRaw = hasNonEmptyValue(rotationGapInput)
  ? Number(rotationGapInput)
  : currentSettings.rotationGapMinutes;

 if (scheduleTime === null) return badRequest(res, 'Invalid schedule time');
 if (runtimeEndTime === null) return badRequest(res, 'Invalid runtime end time');
 const scheduleStartMinutes = timeToMinutes(scheduleTime);
 const scheduleEndMinutes = timeToMinutes(runtimeEndTime);
 if (scheduleStartMinutes === null || scheduleEndMinutes === null || scheduleEndMinutes < scheduleStartMinutes) {
  return badRequest(res, 'Runtime end time must be after start time');
 }
 if (!Number.isFinite(minIntervalMinutesRaw) || minIntervalMinutesRaw < 1) {
  return badRequest(res, 'Invalid min interval');
 }
 if (!Number.isFinite(rotationGapMinutesRaw) || rotationGapMinutesRaw < 0) {
  return badRequest(res, 'Invalid rotation gap');
 }

 setSetting('scheduleEnabled', scheduleEnabled === 0 || scheduleEnabled === false || scheduleEnabled === '0' ? '0' : '1');
 setSetting('scheduleTime', scheduleTime);
 setSetting('runtimeEndTime', runtimeEndTime);
 setSetting('minIntervalMinutes', String(Math.floor(minIntervalMinutesRaw)));
 setSetting('rotationGapMinutes', String(Math.floor(rotationGapMinutesRaw)));
 invalidateDerivedCaches({ analytics: false });

 configureScheduleJob();
 let runNowResult = null;
 if (runNow) {
  runNowResult = generateDailySchedule(runDate, true);
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
  const result = generateDailySchedule(date, startFromNow);
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
 const endTimeRaw = req.query?.endTime;
 const minIntervalRaw = req.query?.minIntervalMinutes;
 const rotationGapRaw = req.query?.rotationGapMinutes;
 const startFromNowRaw = req.query?.startFromNow;

 const hasStartTime = hasNonEmptyValue(startTimeRaw);
 const hasEndTime = hasNonEmptyValue(endTimeRaw);
 const hasMinInterval = hasNonEmptyValue(minIntervalRaw);
 const hasRotationGap = hasNonEmptyValue(rotationGapRaw);
 const startFromNow = startFromNowRaw === '1' || startFromNowRaw === 1 || startFromNowRaw === true || startFromNowRaw === 'true';

 const scheduleTime = hasStartTime ? normalizeTime(startTimeRaw) : settings.scheduleTime;
 const runtimeEndTime = hasEndTime ? normalizeTime(endTimeRaw) : settings.runtimeEndTime;
 const minIntervalMinutes = hasMinInterval ? Number(minIntervalRaw) : settings.minIntervalMinutes;
 const rotationGapMinutes = hasRotationGap ? Number(rotationGapRaw) : settings.rotationGapMinutes;

 if (hasStartTime && scheduleTime === null) return badRequest(res, 'Invalid start time');
 if (hasEndTime && runtimeEndTime === null) return badRequest(res, 'Invalid end time');
 if (!Number.isFinite(minIntervalMinutes) || minIntervalMinutes < 1) {
  return badRequest(res, 'Invalid min interval');
 }
 if (!Number.isFinite(rotationGapMinutes) || rotationGapMinutes < 0) {
  return badRequest(res, 'Invalid rotation gap');
 }

 const scheduleStartMinutes = timeToMinutes(scheduleTime);
 const scheduleEndMinutes = timeToMinutes(runtimeEndTime);
 if (scheduleStartMinutes === null || scheduleEndMinutes === null || scheduleEndMinutes < scheduleStartMinutes) {
  return badRequest(res, 'Runtime end time must be after start time');
 }

const forecastConfig = {
 scheduleTime,
 runtimeEndTime,
 minIntervalMinutes: Math.floor(minIntervalMinutes),
 rotationGapMinutes: Math.floor(rotationGapMinutes)
};
 res.send(getCachedScheduleForecast(targetDate, forecastConfig, { startFromNow }));
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
 ensureTodaySchedule();
 runtimeTickHandle = setInterval(() => {
  processDueSchedule().catch((e) => console.log('Schedule tick failed:', e.message));
 }, 30000);
}

function stopRuntimeWorkers() {
 if (runtimeTickHandle) {
  clearInterval(runtimeTickHandle);
  runtimeTickHandle = null;
 }
 if (scheduleJob) {
  scheduleJob.stop();
  scheduleJob = null;
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
