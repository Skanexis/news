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
 sessionIndex = 1
} = {}) {
 const createdAt = new Date().toISOString();
 const result = db.prepare(`
  INSERT INTO schedule_items (postId,scheduledAt,status,error,createdAt,sentAt,processingStartedAt,sessionIndex)
  VALUES (?,?,?,?,?,?,?,?)
 `).run(postId, scheduledAt, status, null, createdAt, null, processingStartedAt, sessionIndex);
 return Number(result.lastInsertRowid);
}

function minutesToClock(minutes) {
 const normalized = Math.max(0, Math.floor(minutes));
 const hh = String(Math.floor(normalized / 60) % 24).padStart(2, '0');
 const mm = String(normalized % 60).padStart(2, '0');
 return `${hh}:${mm}`;
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

test('buildRuntimePlan schedules two daily sessions with 5 minute interval', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 11, companyId: 1, companyName: 'Alpha' },
  { id: 22, companyId: 2, companyName: 'Beta' },
  { id: 33, companyId: 3, companyName: 'Gamma' }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  secondScheduleTime: '18:00',
  minIntervalMinutes: 5
 }, {
  startFromNow: false
 });

 assert.equal(plan.totalPublications, 6);
 assert.equal(plan.sessions.length, 2);
 const firstSessionMinutes = plan.slots.filter((slot) => slot.sessionIndex === 1).map((slot) => slot.minutes);
 const secondSessionMinutes = plan.slots.filter((slot) => slot.sessionIndex === 2).map((slot) => slot.minutes);
 const firstSessionPostIds = plan.slots.filter((slot) => slot.sessionIndex === 1).map((slot) => Number(slot.post?.id || 0));
 const secondSessionPostIds = plan.slots.filter((slot) => slot.sessionIndex === 2).map((slot) => Number(slot.post?.id || 0));
 assert.deepEqual(firstSessionMinutes, [540, 545, 550]);
 assert.deepEqual(secondSessionMinutes, [1080, 1085, 1090]);
 assert.equal(new Set(firstSessionPostIds).size, posts.length);
 assert.equal(new Set(secondSessionPostIds).size, posts.length);
 assert.deepEqual([...firstSessionPostIds].sort((a, b) => a - b), [11, 22, 33]);
 assert.deepEqual([...secondSessionPostIds].sort((a, b) => a - b), [11, 22, 33]);
 assert.notDeepEqual(firstSessionPostIds, secondSessionPostIds);
 assert.equal(plan.perPostCounts.get(11), 2);
 assert.equal(plan.perPostCounts.get(22), 2);
 assert.equal(plan.perPostCounts.get(33), 2);
});

test('buildRuntimePlan marks overflow when session starts too late', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 101, companyId: 1, companyName: 'A' },
  { id: 102, companyId: 2, companyName: 'B' },
  { id: 103, companyId: 3, companyName: 'C' }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '23:55',
  secondScheduleTime: '23:58',
  minIntervalMinutes: 5
 }, {
  startFromNow: false
 });

 assert.equal(plan.totalPublications, 2);
 assert.equal(plan.sessions[0].plannedPublications, 1);
 assert.equal(plan.sessions[0].overflowSkipped, 2);
 assert.equal(plan.sessions[1].plannedPublications, 1);
 assert.equal(plan.sessions[1].overflowSkipped, 2);
});

test('buildRuntimePlan can start immediately for a past session', () => {
 const today = getCurrentDateString();
 const nowIso = String(mod.nowLocalIso());
 const hh = Number(nowIso.slice(11, 13) || 0);
 const mm = Number(nowIso.slice(14, 16) || 0);
 const nowMinutes = (hh * 60) + mm;
 const pastMinutes = nowMinutes > 0 ? nowMinutes - 1 : 0;
 const futureMinutes = Math.min(nowMinutes + 10, 23 * 60 + 59);
 const secondStart = futureMinutes === pastMinutes ? Math.min(futureMinutes + 1, 23 * 60 + 59) : futureMinutes;
 const plan = buildRuntimePlan(today, [{ id: 1, companyId: 1, companyName: 'A' }], {
  scheduleTime: minutesToClock(pastMinutes),
  secondScheduleTime: minutesToClock(secondStart),
  minIntervalMinutes: 5
 }, {
  startFromNow: true,
  forceImmediateSession: true
 });

 const hasImmediate = plan.sessions.some((session) => session.mode === 'immediate');
 assert.equal(hasImmediate, nowMinutes > 0);
});

test('buildRuntimePlan respects custom interval from settings', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 901, companyId: 1, companyName: 'A' },
  { id: 902, companyId: 2, companyName: 'B' },
  { id: 903, companyId: 3, companyName: 'C' }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  secondScheduleTime: '10:00',
  minIntervalMinutes: 12
 }, {
  startFromNow: false
 });
 const firstSessionMinutes = plan.slots.filter((slot) => slot.sessionIndex === 1).map((slot) => slot.minutes);
 assert.deepEqual(firstSessionMinutes, [540, 552, 564]);
});

test('buildRuntimePlan works with only first session configured', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 1001, companyId: 1, companyName: 'A' },
  { id: 1002, companyId: 2, companyName: 'B' },
  { id: 1003, companyId: 3, companyName: 'C' }
 ];
 const plan = buildRuntimePlan(today, posts, {
  scheduleTime: '09:00',
  secondScheduleTime: null,
  minIntervalMinutes: 5
 }, {
  startFromNow: false
 });
 assert.equal(plan.sessions.length, 1);
 assert.equal(plan.totalPublications, 3);
 const onlySessionIds = plan.slots.map((slot) => Number(slot.post?.id || 0));
 assert.equal(new Set(onlySessionIds).size, posts.length);
});

test('buildRuntimePlan keeps deterministic shuffle for same date/config', () => {
 const today = getCurrentDateString();
 const posts = [
  { id: 1, companyId: 1, companyName: 'A' },
  { id: 2, companyId: 2, companyName: 'B' },
  { id: 3, companyId: 3, companyName: 'C' },
  { id: 4, companyId: 4, companyName: 'D' }
 ];
 const config = {
  scheduleTime: '09:00',
  secondScheduleTime: '18:00',
  minIntervalMinutes: 5
 };
 const planA = buildRuntimePlan(today, posts, config, { startFromNow: false });
 const planB = buildRuntimePlan(today, posts, config, { startFromNow: false });
 const compactA = planA.slots.map((slot) => `${slot.sessionIndex}:${Number(slot.post?.id || 0)}`);
 const compactB = planB.slots.map((slot) => `${slot.sessionIndex}:${Number(slot.post?.id || 0)}`);
 assert.deepEqual(compactA, compactB);
});
