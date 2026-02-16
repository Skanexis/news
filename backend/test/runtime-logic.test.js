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

function seedSentLog({ companyId, postId = null, createdAt = new Date().toISOString() } = {}) {
 const date = String(createdAt).slice(0, 10);
 db.prepare(`
  INSERT INTO logs
   (postId,companyId,companyName,platform,date,status,error,createdAt,trigger,publishedAt,sentChatId,sentMessageId,sentViews,viewsUpdatedAt)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
 `).run(
  postId,
  companyId,
  `Company ${companyId || 'N/A'}`,
  'telegram',
  date,
  'sent',
  null,
  createdAt,
  'manual',
  createdAt,
  null,
  null,
  null,
  null
 );
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

test('buildRuntimePlan avoids consecutive slots from the same company when alternatives exist', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 101, companyId: 1, companyName: 'Alpha', companyPremium: 0, preferredTime: null },
  { id: 102, companyId: 1, companyName: 'Alpha', companyPremium: 0, preferredTime: null },
  { id: 201, companyId: 2, companyName: 'Beta', companyPremium: 0, preferredTime: null }
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

 assert.ok(plan.slots.length >= 2);
 for (let i = 1; i < plan.slots.length; i += 1) {
  assert.notEqual(plan.slots[i].post.companyId, plan.slots[i - 1].post.companyId);
 }
});

test('buildRuntimePlan prefers non-consecutive company even when preferred post is due', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 301, companyId: 10, companyName: 'Alpha', companyPremium: 0, preferredTime: '09:00' },
  { id: 302, companyId: 10, companyName: 'Alpha', companyPremium: 0, preferredTime: '09:20' },
  { id: 401, companyId: 20, companyName: 'Beta', companyPremium: 0, preferredTime: null }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  runtimeEndTime: '10:00',
  minIntervalMinutes: 20,
  rotationGapMinutes: 0
 }, {
  startFromNow: false,
  startCursor: 0
 });

 assert.ok(plan.slots.length >= 3);
 assert.equal(plan.slots[0].post.companyId, 10);
 assert.equal(plan.slots[1].post.companyId, 20);
 assert.equal(plan.slots[1].source, 'rotation');
});

test('buildRuntimePlan still schedules when only one company is available', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 501, companyId: 77, companyName: 'Solo', companyPremium: 0, preferredTime: null },
  { id: 502, companyId: 77, companyName: 'Solo', companyPremium: 0, preferredTime: null }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  runtimeEndTime: '10:00',
  minIntervalMinutes: 20,
  rotationGapMinutes: 0
 }, {
  startFromNow: false,
  startCursor: 0
 });

 assert.ok(plan.slots.length >= 2);
 const allSameCompany = plan.slots.every((slot) => slot.post.companyId === 77);
 assert.equal(allSameCompany, true);
});

test('buildRuntimePlan prioritizes under-served regular company when historical gap is large', () => {
 const today = getCurrentDateString();
 for (let i = 0; i < 40; i += 1) {
  seedSentLog({ companyId: 1, postId: 9000 + i });
 }
 const posts = [
  { id: 601, companyId: 1, companyName: 'Alpha', companyPremium: 0, preferredTime: null },
  { id: 602, companyId: 2, companyName: 'Beta', companyPremium: 0, preferredTime: null }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  runtimeEndTime: '09:40',
  minIntervalMinutes: 20,
  rotationGapMinutes: 0
 }, {
  startFromNow: false,
  startCursor: 0
 });

 const companySequence = plan.slots.map((slot) => Number(slot.post.companyId || 0));
 assert.ok(companySequence.length >= 2);
 assert.equal(companySequence[0], 2);
 const alphaCount = companySequence.filter((companyId) => companyId === 1).length;
 const betaCount = companySequence.filter((companyId) => companyId === 2).length;
 assert.ok(betaCount >= alphaCount);
});

test('buildRuntimePlan keeps premium company ahead with x1.5 weighting', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 701, companyId: 10, companyName: 'Alpha', companyPremium: 0, preferredTime: null },
  { id: 702, companyId: 20, companyName: 'Beta', companyPremium: 0, preferredTime: null },
  { id: 703, companyId: 30, companyName: 'Gamma', companyPremium: 1, preferredTime: null }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  runtimeEndTime: '09:59',
  minIntervalMinutes: 1,
  rotationGapMinutes: 0
 }, {
  startFromNow: false,
  startCursor: 0
 });

 const counts = new Map();
 for (const slot of plan.slots) {
  const companyId = Number(slot.post.companyId || 0);
  counts.set(companyId, (counts.get(companyId) || 0) + 1);
 }
 const alphaCount = counts.get(10) || 0;
 const betaCount = counts.get(20) || 0;
 const gammaPremiumCount = counts.get(30) || 0;

 assert.ok(gammaPremiumCount > alphaCount);
 assert.ok(gammaPremiumCount > betaCount);

 const normalizedShares = [
  alphaCount / 2,
  betaCount / 2,
  gammaPremiumCount / 3
 ];
 const maxShare = Math.max(...normalizedShares);
 const minShare = Math.min(...normalizedShares);
 assert.ok((maxShare - minShare) <= 1);
});
