import test, { beforeEach, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'yosupport-runtime-test-'));
const dbPath = path.join(tempDir, 'runtime.sqlite');

process.env.NODE_ENV = 'test';
process.env.DISABLE_RUNTIME = '1';
process.env.DB_PATH = dbPath;

const mod = await import(new URL(`../index.js?test=${Date.now()}`, import.meta.url).href);
const {
 db,
 setSetting,
 getCurrentDateString,
 buildRuntimePlan,
 claimNextDueScheduleItem,
 markInvalidPendingScheduleItems,
 reclaimStaleProcessingItems,
 processDueSchedule
} = mod;

function resetDb() {
 const tables = [
  'schedule_items',
  'logs',
  'link_clicks',
  'links',
  'posts',
  'companies',
  'drafts',
  'settings'
 ];
 for (const table of tables) {
  db.prepare(`DELETE FROM ${table}`).run();
 }
}

function seedCompany({ name = 'ACME', preferredTime = null, premium = 0 } = {}) {
 const result = db.prepare('INSERT INTO companies (name, preferredTime, premium) VALUES (?,?,?)')
  .run(name, preferredTime, premium);
 return Number(result.lastInsertRowid);
}

function seedPost({
 companyId,
 startDate,
 endDate,
 active = 1
} = {}) {
 const result = db.prepare(`
  INSERT INTO posts (companyId,text,platform,startDate,endDate,active,draftId,ctaUrl,ctaLabel,trackLinks,linkId,buttons)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
 `).run(
  companyId,
  'Test post',
  'telegram',
  startDate,
  endDate,
  active,
  null,
  null,
  null,
  0,
  null,
  null
 );
 return Number(result.lastInsertRowid);
}

function seedScheduleItem({
 postId,
 scheduledAt,
 status = 'pending',
 processingStartedAt = null,
 rotationCursorAfter = 0
} = {}) {
 const createdAt = new Date().toISOString();
 const result = db.prepare(`
  INSERT INTO schedule_items (postId,scheduledAt,status,error,createdAt,sentAt,processingStartedAt,rotationCursorAfter)
  VALUES (?,?,?,?,?,?,?,?)
 `).run(postId, scheduledAt, status, null, createdAt, null, processingStartedAt, rotationCursorAfter);
 return Number(result.lastInsertRowid);
}

beforeEach(() => {
 resetDb();
});

after(() => {
 try {
  db.close();
 } catch (e) {}
 try {
  fs.rmSync(tempDir, { recursive: true, force: true });
 } catch (e) {}
});

test('claimNextDueScheduleItem claims single pending item once', () => {
 const today = getCurrentDateString();
 const companyId = seedCompany();
 const postId = seedPost({ companyId, startDate: today, endDate: today, active: 1 });
 const scheduleId = seedScheduleItem({ postId, scheduledAt: `${today}T09:00:00` });

 const firstClaim = claimNextDueScheduleItem(`${today}T10:00:00`, today);
 const secondClaim = claimNextDueScheduleItem(`${today}T10:00:00`, today);

 assert.equal(firstClaim, scheduleId);
 assert.equal(secondClaim, null);
 const row = db.prepare('SELECT status FROM schedule_items WHERE id = ?').get(scheduleId);
 assert.equal(row.status, 'processing');
});

test('markInvalidPendingScheduleItems fails pending items for inactive posts', () => {
 const today = getCurrentDateString();
 const companyId = seedCompany();
 const postId = seedPost({ companyId, startDate: today, endDate: today, active: 1 });
 const scheduleId = seedScheduleItem({ postId, scheduledAt: `${today}T11:00:00` });

 db.prepare('UPDATE posts SET active = 0 WHERE id = ?').run(postId);
 markInvalidPendingScheduleItems();

 const row = db.prepare('SELECT status,error FROM schedule_items WHERE id = ?').get(scheduleId);
 assert.equal(row.status, 'failed');
 assert.match(String(row.error || ''), /inactive or outside campaign date/i);
});

test('processDueSchedule does not claim/send when scheduler is disabled', async () => {
 const today = getCurrentDateString();
 const companyId = seedCompany();
 const postId = seedPost({ companyId, startDate: today, endDate: today, active: 1 });
 const scheduleId = seedScheduleItem({ postId, scheduledAt: `${today}T10:00:00` });

 setSetting('scheduleEnabled', '0');
 setSetting('minIntervalMinutes', '1');
 await processDueSchedule(false);

 const row = db.prepare('SELECT status FROM schedule_items WHERE id = ?').get(scheduleId);
 assert.equal(row.status, 'pending');
});

test('reclaimStaleProcessingItems returns stale processing tasks back to pending', () => {
 const today = getCurrentDateString();
 const companyId = seedCompany();
 const postId = seedPost({ companyId, startDate: today, endDate: today, active: 1 });
 const staleStart = new Date(Date.now() - (40 * 60 * 1000)).toISOString();
 const scheduleId = seedScheduleItem({
  postId,
  scheduledAt: `${today}T12:00:00`,
  status: 'processing',
  processingStartedAt: staleStart
 });

 reclaimStaleProcessingItems(10);
 const row = db.prepare('SELECT status,processingStartedAt,error FROM schedule_items WHERE id = ?').get(scheduleId);
 assert.equal(row.status, 'pending');
 assert.equal(row.processingStartedAt, null);
 assert.match(String(row.error || ''), /recovered/i);
});

test('failed send does not set lastSentAt throttle marker', async () => {
 const previousToken = process.env.TELEGRAM_BOT_TOKEN;
 process.env.TELEGRAM_BOT_TOKEN = '';
 try {
   const today = getCurrentDateString();
   const companyId = seedCompany();
   const postId = seedPost({ companyId, startDate: today, endDate: today, active: 1 });
   seedScheduleItem({ postId, scheduledAt: `${today}T00:00:00` });

   setSetting('scheduleEnabled', '1');
   setSetting('minIntervalMinutes', '1');
   await processDueSchedule(true);

   const lastSentAt = db.prepare('SELECT value FROM settings WHERE key = ?').get('lastSentAt');
   assert.equal(lastSentAt, undefined);
   const failedCount = db.prepare('SELECT COUNT(*) as c FROM schedule_items WHERE status = ?').get('failed').c;
   assert.equal(Number(failedCount), 1);
 } finally {
  if (previousToken === undefined) {
   delete process.env.TELEGRAM_BOT_TOKEN;
  } else {
   process.env.TELEGRAM_BOT_TOKEN = previousToken;
  }
 }
});

test('buildRuntimePlan does not place preferred post before preferred time', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 11, companyName: 'Alpha', companyPremium: 0, preferredTime: '10:00' },
  { id: 22, companyName: 'Beta', companyPremium: 0, preferredTime: null }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  runtimeEndTime: '10:40',
  minIntervalMinutes: 20,
  rotationGapMinutes: 0
 }, {
  startFromNow: false,
  startCursor: 0
 });

 const alphaSlotsBefore10 = plan.slots.filter((slot) => slot.post.id === 11 && slot.minutes < 600);
 assert.equal(alphaSlotsBefore10.length, 0);
 const alphaSlots = plan.slots.filter((slot) => slot.post.id === 11);
 assert.ok(alphaSlots.length >= 1);
});
