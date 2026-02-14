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

app.use((req, res, next) => {
 res.setHeader('Access-Control-Allow-Origin', '*');
 res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
 res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
 if (req.method === 'OPTIONS') return res.sendStatus(200);
 next();
});

const db = new Database('db.sqlite');
db.pragma('journal_mode = WAL');

const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || 'http://localhost:3000').replace(/\/$/, '');
const CRON_TIME = process.env.CRON_TIME || '0 9 * * *';
const CRON_TZ = process.env.CRON_TZ || null;
const ANALYTICS_SALT = process.env.ANALYTICS_SALT || '';

const uploadsDir = path.join(process.cwd(), 'uploads');
if (!fs.existsSync(uploadsDir)) {
 fs.mkdirSync(uploadsDir, { recursive: true });
}

const allowedPlatforms = new Set(['telegram']);

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

function getCtaUrl(post) {
 return normalizeUrl(post.ctaUrl) || normalizeUrl(post.telegramPublicUrl) || null;
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

function timeToMinutes(value) {
 const normalized = normalizeTime(value);
 if (!normalized) return null;
 const [h, m] = normalized.split(':').map(Number);
 return h * 60 + m;
}

function minutesToIso(date, minutes) {
 const base = new Date(`${date}T00:00:00`);
 base.setMinutes(base.getMinutes() + minutes);
 const pad = (num) => String(num).padStart(2, '0');
 return `${base.getFullYear()}-${pad(base.getMonth() + 1)}-${pad(base.getDate())}T${pad(base.getHours())}:${pad(base.getMinutes())}:00`;
}

function nowLocalIso() {
 const now = new Date();
 const pad = (num) => String(num).padStart(2, '0');
 return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}T${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
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
 const minIntervalMinutes = Number(getSetting('minIntervalMinutes') || 20);
 const enabledRaw = getSetting('scheduleEnabled');
 const enabled = enabledRaw === null ? true : enabledRaw !== '0';
 return {
  scheduleTime,
  minIntervalMinutes: Number.isFinite(minIntervalMinutes) && minIntervalMinutes > 0 ? Math.floor(minIntervalMinutes) : 20,
  enabled
 };
}

async function telegramApi(method, payload = {}) {
 if (!process.env.TELEGRAM_BOT_TOKEN) {
  throw new Error('Telegram bot token not set');
 }
 const url = `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/${method}`;
 const response = await fetch(url, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(payload)
 });
 const data = await response.json().catch(() => ({}));
 if (!response.ok || !data.ok) {
  const message = data.description || data.error || response.statusText || 'Telegram API error';
  throw new Error(message);
 }
 return data.result;
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

async function handleTelegramMessage(message) {
 if (!message || !message.chat || !message.message_id) return;
 const chatId = String(message.chat.id);
 const messageId = message.message_id;
 const text = message.text || null;
 const caption = message.caption || null;
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
   mediaType = ?, text = ?, caption = ?, fileId = ?, fileUniqueId = ?, fileName = ?, mimeType = ?, filePath = ?, createdAt = ?
   WHERE id = ?
  `).run(mediaType, text, caption, fileId, fileUniqueId, fileName, mimeType, filePath, createdAt, existing.id);
  return existing.id;
 }

 const result = db.prepare(`INSERT INTO drafts
  (source, chatId, messageId, mediaType, text, caption, fileId, fileUniqueId, fileName, mimeType, filePath, createdAt)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
 `).run('telegram', chatId, messageId, mediaType, text, caption, fileId, fileUniqueId, fileName, mimeType, filePath, createdAt);
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
 const chatId = post.telegramChannelId || process.env.TELEGRAM_CHANNEL_ID;
 if (!chatId) throw new Error('Company Telegram channel ID is required');
 const ctaUrl = resolveCtaUrl(post);
 const ctaLabel = formatCtaLabel(ctaUrl, post.ctaLabel);
 const replyMarkup = ctaUrl ? { inline_keyboard: [[{ text: ctaLabel, url: ctaUrl }]] } : null;

 if (post.draftId) {
  const draft = db.prepare('SELECT * FROM drafts WHERE id = ?').get(post.draftId);
  if (!draft) throw new Error('Draft not found');
  const payload = {
   chat_id: chatId,
   from_chat_id: draft.chatId,
   message_id: draft.messageId
  };
  if (replyMarkup) payload.reply_markup = replyMarkup;
  await telegramApi('copyMessage', payload);
  return;
 }

 if (!post.text) throw new Error('Post text is required');
 const payload = { chat_id: chatId, text: post.text };
 if (replyMarkup) payload.reply_markup = replyMarkup;
 await telegramApi('sendMessage', payload);
}

function insertLog(post, status, error, trigger, dateOverride) {
 const createdAt = new Date().toISOString();
 const date = dateOverride || createdAt.slice(0, 10);
 db.prepare('INSERT INTO logs (postId,platform,date,status,error,createdAt,trigger) VALUES (?,?,?,?,?,?,?)')
  .run(post.id, post.platform, date, status, error || null, createdAt, trigger || 'schedule');
}

function generateDailySchedule(date, startFromNow = true) {
 const targetDate = date || new Date().toISOString().slice(0, 10);
 const settings = getSchedulerSettings();
 const posts = db.prepare(`
  SELECT posts.*, 
    companies.name as companyName,
    companies.telegramChannelId,
    companies.telegramPublicUrl,
    companies.preferredTime
    , links.code as linkCode
   FROM posts
   JOIN companies ON posts.companyId = companies.id
   LEFT JOIN links ON posts.linkId = links.id
   WHERE posts.active=1 AND posts.startDate<=? AND posts.endDate>=? AND posts.platform = 'telegram'
 `).all(targetDate, targetDate);

 if (!posts.length) return { date: targetDate, total: 0, scheduled: 0 };

 const dayStart = `${targetDate}T00:00:00`;
 const dayEnd = `${targetDate}T23:59:59.999`;
 db.prepare(`DELETE FROM schedule_items WHERE scheduledAt >= ? AND scheduledAt <= ? AND status = 'pending'`)
  .run(dayStart, dayEnd);

 const defaultMinutes = timeToMinutes(settings.scheduleTime) ?? 9 * 60;
 const isToday = targetDate === new Date().toISOString().slice(0, 10);
 const now = new Date();
 const nowMinutes = isToday ? now.getHours() * 60 + now.getMinutes() : null;
 const baseMinutes = startFromNow && isToday && nowMinutes !== null ? nowMinutes : null;
 const minInterval = Math.max(1, settings.minIntervalMinutes);

 const items = posts.map((post) => {
  const desired = timeToMinutes(post.preferredTime) ?? defaultMinutes;
  return { post, desired };
 }).sort((a, b) => a.desired - b.desired || a.post.id - b.post.id);

 let lastAssigned = baseMinutes !== null ? baseMinutes - minInterval : -Infinity;
 const createdAt = new Date().toISOString();
 let scheduled = 0;

 for (const entry of items) {
  let target = entry.desired;
  if (baseMinutes !== null) target = Math.max(target, baseMinutes);
  const assigned = Math.max(target, lastAssigned + minInterval);
  lastAssigned = assigned;
  const scheduledAt = minutesToIso(targetDate, assigned);
  db.prepare(`INSERT INTO schedule_items (postId, scheduledAt, status, createdAt) VALUES (?,?,?,?)`)
   .run(entry.post.id, scheduledAt, 'pending', createdAt);
  scheduled += 1;
 }

 return { date: targetDate, total: posts.length, scheduled };
}

let scheduleProcessing = false;
async function processDueSchedule(force = false) {
 if (scheduleProcessing) return;
 scheduleProcessing = true;
 try {
  const settings = getSchedulerSettings();
  const minIntervalMs = Math.max(1, settings.minIntervalMinutes) * 60 * 1000;
  const lastSentAt = getSetting('lastSentAt');
  if (lastSentAt) {
   const delta = Date.now() - new Date(lastSentAt).getTime();
   if (delta < minIntervalMs) return;
  }

  const nowIso = nowLocalIso();
    const item = db.prepare(`
     SELECT
      schedule_items.id as scheduleId,
      schedule_items.postId as schedulePostId,
      schedule_items.scheduledAt,
      schedule_items.status,
      schedule_items.error,
      schedule_items.createdAt,
      schedule_items.sentAt,
      posts.*,
      companies.name as companyName,
      companies.telegramChannelId,
      companies.telegramPublicUrl,
      links.code as linkCode
     FROM schedule_items
     JOIN posts ON schedule_items.postId = posts.id
     JOIN companies ON posts.companyId = companies.id
     LEFT JOIN links ON posts.linkId = links.id
     WHERE schedule_items.status='pending' AND schedule_items.scheduledAt <= ? AND posts.platform = 'telegram'
     ORDER BY schedule_items.scheduledAt ASC
     LIMIT 1
    `).get(nowIso);

  if (!item) return;

  const sentAt = new Date().toISOString();
  try {
     if (item.platform === 'telegram') await sendTelegramPost(item);
     db.prepare('UPDATE schedule_items SET status=?, sentAt=? WHERE id=?')
      .run('sent', sentAt, item.scheduleId);
     insertLog(item, 'sent', null, 'auto');
    } catch (e) {
     db.prepare('UPDATE schedule_items SET status=?, sentAt=?, error=? WHERE id=?')
      .run('failed', sentAt, e.message, item.scheduleId);
     insertLog(item, 'failed', e.message, 'auto');
    }
  setSetting('lastSentAt', sentAt);
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
 const today = new Date().toISOString().slice(0, 10);
 const dayStart = `${today}T00:00:00`;
 const dayEnd = `${today}T23:59:59.999`;
 const existing = db.prepare('SELECT COUNT(*) as count FROM schedule_items WHERE scheduledAt >= ? AND scheduledAt <= ?')
  .get(dayStart, dayEnd)?.count || 0;
 if (existing > 0) return;
 const [hh, mm] = scheduleTime.split(':').map(Number);
 const scheduleMinutes = hh * 60 + mm;
 const now = new Date();
 const nowMinutes = now.getHours() * 60 + now.getMinutes();
 if (nowMinutes >= scheduleMinutes) {
  generateDailySchedule(today, true);
 }
}

async function publishPostNow(postId) {
 const post = db.prepare(`
 SELECT posts.*, 
   companies.name as companyName,
   companies.telegramChannelId,
   companies.telegramPublicUrl,
   , links.code as linkCode
  FROM posts
  JOIN companies ON posts.companyId = companies.id
  LEFT JOIN links ON posts.linkId = links.id
  WHERE posts.id = ?
 `).get(postId);

 if (!post) throw new Error('Post not found');

 try {
  if (post.platform !== 'telegram') throw new Error('Unsupported platform');
  await sendTelegramPost(post);
  insertLog(post, 'sent', null, 'manual');
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
 preferredTime TEXT
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
 linkId INTEGER
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
 sentAt TEXT
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS logs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 postId INTEGER,
 platform TEXT,
 date TEXT,
 status TEXT,
 error TEXT,
 createdAt TEXT,
 trigger TEXT
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
 createdAt TEXT
)`).run();

db.prepare(`CREATE TABLE IF NOT EXISTS settings (
 key TEXT PRIMARY KEY,
 value TEXT
)`).run();

ensureColumn('companies', 'telegramChannelId', 'TEXT');
ensureColumn('companies', 'telegramPublicUrl', 'TEXT');
ensureColumn('companies', 'preferredTime', 'TEXT');

ensureColumn('posts', 'draftId', 'INTEGER');
ensureColumn('posts', 'ctaUrl', 'TEXT');
ensureColumn('posts', 'ctaLabel', 'TEXT');
ensureColumn('posts', 'trackLinks', 'INTEGER');
ensureColumn('posts', 'linkId', 'INTEGER');
ensureColumn('logs', 'createdAt', 'TEXT');
ensureColumn('logs', 'trigger', 'TEXT');

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
ensureColumn('drafts', 'createdAt', 'TEXT');

app.use('/uploads', express.static(uploadsDir));
configureScheduleJob();
ensureTodaySchedule();
setInterval(() => {
 processDueSchedule().catch((e) => console.log('Schedule tick failed:', e.message));
}, 30000);

app.get('/health', (_, res) => res.send({ ok: true }));

app.get('/settings', (_, res) => {
 const settings = getSchedulerSettings();
 res.send({
  scheduleEnabled: settings.enabled ? 1 : 0,
  scheduleTime: settings.scheduleTime,
  minIntervalMinutes: settings.minIntervalMinutes
 });
});

app.put('/settings', (req, res) => {
 const scheduleEnabled = req.body?.scheduleEnabled;
 const scheduleTime = normalizeTime(req.body?.scheduleTime);
 const minIntervalMinutesRaw = Number(req.body?.minIntervalMinutes);

 if (scheduleTime === null) return badRequest(res, 'Invalid schedule time');
 if (!Number.isFinite(minIntervalMinutesRaw) || minIntervalMinutesRaw < 1) {
  return badRequest(res, 'Invalid min interval');
 }

 setSetting('scheduleEnabled', scheduleEnabled === 0 || scheduleEnabled === false || scheduleEnabled === '0' ? '0' : '1');
 setSetting('scheduleTime', scheduleTime);
 setSetting('minIntervalMinutes', String(Math.floor(minIntervalMinutesRaw)));

 configureScheduleJob();
 res.send({ ok: true });
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
  companies.telegramChannelId,
  companies.telegramPublicUrl,
  companies.preferredTime,
  (SELECT COUNT(*) FROM posts WHERE posts.companyId = companies.id AND posts.platform = 'telegram') as postCount
 FROM companies
 ORDER BY companies.id DESC
`).all()));

app.post('/companies', (req, res) => {
 const name = normalizeText(req.body.name);
 if (!name) return badRequest(res, 'Company name is required');
 const telegramChannelId = normalizeText(req.body.telegramChannelId);
 const telegramPublicUrl = normalizeUrl(req.body.telegramPublicUrl);
 const preferredTime = normalizeTime(req.body.preferredTime);
 if (req.body.preferredTime && !preferredTime) return badRequest(res, 'Invalid preferred time');

 db.prepare(`INSERT INTO companies (name, telegramChannelId, telegramPublicUrl, preferredTime)
  VALUES (?,?,?,?)
 `).run(name, telegramChannelId, telegramPublicUrl, preferredTime);
 res.send({ ok: true });
});

app.put('/companies/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid company id');
 const name = normalizeText(req.body.name);
 if (!name) return badRequest(res, 'Company name is required');
 const telegramChannelId = normalizeText(req.body.telegramChannelId);
 const telegramPublicUrl = normalizeUrl(req.body.telegramPublicUrl);
 const preferredTime = normalizeTime(req.body.preferredTime);
 if (req.body.preferredTime && !preferredTime) return badRequest(res, 'Invalid preferred time');

 db.prepare(`UPDATE companies SET
  name=?, telegramChannelId=?, telegramPublicUrl=?, preferredTime=?
  WHERE id=?
 `).run(name, telegramChannelId, telegramPublicUrl, preferredTime, id);
 res.send({ ok: true });
});

app.delete('/companies/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid company id');
 const count = db.prepare('SELECT COUNT(*) as count FROM posts WHERE companyId = ? AND platform = \'telegram\'').get(id).count;
 if (count > 0) return badRequest(res, 'Company has posts. Delete or move posts first.');
 db.prepare('DELETE FROM companies WHERE id = ?').run(id);
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
   companies.name as companyName,
   companies.telegramChannelId,
   companies.telegramPublicUrl,
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
  ORDER BY posts.id DESC
 `).all();
 const enriched = rows.map((row) => ({
  ...row,
  trackedUrl: buildTrackedUrl(row.linkCode)
 }));
 res.send(enriched);
});

app.post('/posts', (req, res) => {
 const { companyId, text, startDate, endDate, active, draftId, ctaUrl, ctaLabel, trackLinks } = req.body;
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

 const insert = db.prepare(`INSERT INTO posts (companyId,text,platform,startDate,endDate,active,draftId,ctaUrl,ctaLabel,trackLinks,linkId)
  VALUES (?,?,?,?,?,?,?,?,?,?,?)
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
  null
 );

 const postId = insert.lastInsertRowid;
 let linkId = null;
 if (trackLinksValue && normalizedCtaUrl) {
  linkId = ensurePostLink(postId, normalizedCtaUrl, null, true);
  if (linkId) {
   db.prepare('UPDATE posts SET linkId=? WHERE id=?').run(linkId, postId);
  }
 }

 res.send({ ok: true, id: postId, linkId });
});

app.put('/posts/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid post id');
 const { companyId, text, startDate, endDate, active, draftId, ctaUrl, ctaLabel, trackLinks } = req.body;
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
 const existing = db.prepare('SELECT linkId, trackLinks FROM posts WHERE id = ?').get(id);
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

 db.prepare(`UPDATE posts SET companyId=?, text=?, platform=?, startDate=?, endDate=?, active=?, draftId=?, ctaUrl=?, ctaLabel=?, trackLinks=?, linkId=? WHERE id=?`)
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
   id
  );
 res.send({ ok: true });
});

app.delete('/posts/:id', (req, res) => {
 const id = Number(req.params.id);
 if (!id) return badRequest(res, 'Invalid post id');
 db.prepare('DELETE FROM posts WHERE id = ?').run(id);
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
 const today = new Date().toISOString().slice(0, 10);
 const base = maxDateString(post.endDate, post.startDate, today) || today;
 const newEndDate = addDaysToDateString(base, 30);
 if (!newEndDate) return res.status(500).send({ ok: false, error: 'Renew failed' });
 db.prepare('UPDATE posts SET endDate=?, active=1 WHERE id=?').run(newEndDate, id);
 res.send({ ok: true, endDate: newEndDate });
});

app.post('/schedule/run', async (req, res) => {
 const date = req.body?.date;
 if (date && !isValidDateString(date)) return badRequest(res, 'Invalid date format');
 try {
  const result = generateDailySchedule(date, true);
  processDueSchedule(true).catch((e) => console.log('Manual schedule send failed:', e.message));
  res.send({ ok: true, result });
 } catch (e) {
  res.status(500).send({ ok: false, error: e.message });
 }
});

app.get('/schedule/items', (req, res) => {
 const date = req.query?.date;
 if (date && !isValidDateString(date)) return badRequest(res, 'Invalid date format');
 const targetDate = date || new Date().toISOString().slice(0, 10);
 const rows = db.prepare(`
  SELECT schedule_items.*,
   posts.platform as postPlatform,
   posts.text as postText,
   posts.ctaLabel as ctaLabel,
   posts.ctaUrl as ctaUrl,
   posts.draftId as draftId,
   drafts.mediaType as draftMediaType,
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

app.get('/drafts', (_, res) => res.send(db.prepare(`
 SELECT * FROM drafts ORDER BY createdAt DESC, id DESC
`).all()));

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

app.get('/logs', (_, res) => res.send(db.prepare(`
 SELECT logs.*,
  posts.text as postText,
  posts.draftId as draftId,
  posts.platform as postPlatform,
  drafts.mediaType as draftMediaType,
  drafts.caption as draftCaption,
  companies.name as companyName
 FROM logs
 LEFT JOIN posts ON logs.postId = posts.id
 LEFT JOIN drafts ON posts.draftId = drafts.id
 LEFT JOIN companies ON posts.companyId = companies.id
 WHERE posts.platform = 'telegram'
 ORDER BY logs.id DESC
`).all()));

app.listen(3000, () => console.log('Backend v7 running'));
