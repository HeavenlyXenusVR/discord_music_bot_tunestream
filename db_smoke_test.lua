-- Live-DB smoke test for tunestream.lua's Postgres write paths (mirrors
-- nexus/sapphire/symphony's db_smoke_test.lua pattern). Runs against the
-- real discord_music_tunestream database (already-migrated, shared with the
-- swarm), so every test below uses a distinct, clearly-fake TEST_GUILD /
-- TEST_USER* id and cleans up its own rows at the end -- it must never
-- touch real guild data (e.g. the pre-existing swarm_health/tunestream_metrics
-- row for guild 1247394560007471134).
--
-- Run with: TUNESTREAM_DRY_RUN=test TUNESTREAM_DB_PORT=5432 luajit db_smoke_test.lua

package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

-- Expects TUNESTREAM_DRY_RUN=test in the environment already (set by the caller,
-- e.g. `TUNESTREAM_DRY_RUN=test TUNESTREAM_DB_PORT=5432 luajit db_smoke_test.lua`).
local A = dofile("tunestream.lua")

local TEST_GUILD = "999900011"
local TEST_USER = "999900012"
local TEST_USER2 = "999900013"

local pass, fail = 0, 0
local function check(label, ok, err)
  if ok then
    pass = pass + 1
    print("OK   " .. label)
  else
    fail = fail + 1
    print("FAIL " .. label .. " -> " .. tostring(err))
  end
end

-- guild settings upsert/read/write
A.ensure_guild_settings(TEST_GUILD)
local settings = A.get_settings(TEST_GUILD)
check("get_settings returns row", settings ~= nil)
A.q("UPDATE tunestream_guild_settings SET volume = %s, loop_mode = %s WHERE guild_id = %s", 77, "song", TEST_GUILD)
settings = A.get_settings(TEST_GUILD)
check("volume persisted", tonumber(settings.volume) == 77, tostring(settings.volume))
check("loop_mode persisted", settings.loop_mode == "song", tostring(settings.loop_mode))

-- queue lifecycle
local uid1 = A.enqueue_track(TEST_GUILD, "https://example.com/a", "Track A", TEST_USER)
local uid2 = A.enqueue_track(TEST_GUILD, "https://example.com/b", "Track B", TEST_USER)
A.insert_queue_front(TEST_GUILD, "https://example.com/z", "Track Z (front)", TEST_USER)
check("queue_count after 3 inserts", A.queue_count(TEST_GUILD) == 3, A.queue_count(TEST_GUILD))
local rows = A.q("SELECT video_url, title FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", TEST_GUILD)
check("insert_queue_front puts Z first", rows and rows[1] and rows[1].title == "Track Z (front)", rows and rows[1] and rows[1].title)

A.snapshot_backup(TEST_GUILD)
local backup_count = A.q1("SELECT COUNT(*)::int AS n FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("backup snapshot matches queue size", backup_count and tonumber(backup_count.n) == 3, backup_count and backup_count.n)

local n = A.shuffle_queue(TEST_GUILD, true)
check("shuffle_queue returns count", n == 3, n)

-- clear queue, then restore from backup
A.q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("queue emptied", A.queue_count(TEST_GUILD) == 0, A.queue_count(TEST_GUILD))
local restored = A.restore_queue_from_backup(TEST_GUILD)
check("restore_queue_from_backup restores 3", restored == 3 and A.queue_count(TEST_GUILD) == 3, restored)

-- delete_backup_track (used by remove/skipto/downvote)
A.delete_backup_track(TEST_GUILD, nil, "https://example.com/z", "Track Z (front)")
local backup_after = A.q1("SELECT COUNT(*)::int AS n FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("delete_backup_track removes one row", backup_after and tonumber(backup_after.n) == 2, backup_after and backup_after.n)

-- track_intelligence / user_track_affinity: every NOT-NULL counter column
-- (queued_count, play_count, finish_count, skip_count, like_count,
-- dislike_count, total_listen_seconds / score) must be zero-filled on first
-- insert, per the live schema introspected via `\d tunestream_track_intelligence`
-- and `\d tunestream_user_track_affinity` -- both confirmed NOT NULL with no
-- default on every counter column.
local ok6, err6 = pcall(A.record_track_feedback, TEST_GUILD, TEST_USER, "https://example.com/a", "Track A", true)
check("record_track_feedback like (NOT NULL counter zero-fill)", ok6, err6)
local ok7, err7 = pcall(A.record_track_feedback, TEST_GUILD, TEST_USER2, "https://example.com/b", "Track B", false)
check("record_track_feedback dislike", ok7, err7)

local key_a = A.track_key("https://example.com/a", "Track A")
local ti = A.q1("SELECT queued_count, play_count, finish_count, like_count, dislike_count, skip_count, total_listen_seconds FROM tunestream_track_intelligence WHERE guild_id = %s AND url_key = %s", TEST_GUILD, key_a)
check("track_intelligence row has all counters non-null",
  ti and ti.queued_count ~= nil and ti.play_count ~= nil and ti.finish_count ~= nil
    and ti.like_count ~= nil and ti.dislike_count ~= nil and ti.skip_count ~= nil and ti.total_listen_seconds ~= nil,
  tostring(ti))
check("track_intelligence like_count == 1 after one like", ti and tonumber(ti.like_count) == 1, ti and tostring(ti.like_count))

local aff = A.q1("SELECT queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score FROM tunestream_user_track_affinity WHERE guild_id = %s AND user_id = %s AND url_key = %s", TEST_GUILD, TEST_USER, key_a)
check("user_track_affinity row has all counters non-null",
  aff and aff.queued_count ~= nil and aff.play_count ~= nil and aff.finish_count ~= nil
    and aff.skip_count ~= nil and aff.like_count ~= nil and aff.dislike_count ~= nil and aff.score ~= nil,
  tostring(aff))
check("user_track_affinity score positive after a like", aff and tonumber(aff.score) > 0, aff and tostring(aff.score))

-- enqueue_track's own track_intelligence queued_count upsert (same NOT-NULL
-- counter shape, exercised inline in enqueue_track rather than a separate fn)
local ti2 = A.q1("SELECT queued_count FROM tunestream_track_intelligence WHERE guild_id = %s AND url_key = %s", TEST_GUILD, A.track_key("https://example.com/a", "Track A"))
check("enqueue_track's queued_count upsert ran (>=1)", ti2 and tonumber(ti2.queued_count) >= 1, ti2 and tostring(ti2.queued_count))

-- autodj toggle
A.set_autodj_enabled(TEST_GUILD, true)
check("autodj enabled true", A.get_autodj_enabled(TEST_GUILD) == true)
A.set_autodj_enabled(TEST_GUILD, false)
check("autodj enabled false", A.get_autodj_enabled(TEST_GUILD) == false)

-- smart recommendation (falls back to "server favorites" / seed fallback query)
local ok9, rec, reason = pcall(A.pick_smart_recommendation, TEST_GUILD, TEST_USER)
check("pick_smart_recommendation runs without error", ok9, rec)

-- playback_state clear (the reset-columns write path used by /clear, /stop's
-- eventual TrackEndEvent settle, and /sleep)
A.q([[INSERT INTO tunestream_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title, play_session_key, track_uid)
      VALUES (%s, 'tunestream', %s, %s, 42, true, false, %s, %s, %s)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET channel_id = EXCLUDED.channel_id, video_url = EXCLUDED.video_url,
        position_seconds = EXCLUDED.position_seconds, is_playing = EXCLUDED.is_playing, title = EXCLUDED.title,
        play_session_key = EXCLUDED.play_session_key, track_uid = EXCLUDED.track_uid]],
  TEST_GUILD, "111", "https://example.com/a", "Track A", uid1, uid1)
local ps = A.q1("SELECT position_seconds, is_playing FROM tunestream_playback_state WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("playback_state upsert persisted", ps and tonumber(ps.position_seconds) == 42 and ps.is_playing == true, tostring(ps))
local ok_clear, err_clear = pcall(A.clear_playback_state, TEST_GUILD)
check("clear_playback_state runs (was inline in /clear before this port)", ok_clear, err_clear)
ps = A.q1("SELECT video_url, is_playing, position_seconds FROM tunestream_playback_state WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("clear_playback_state actually clears the row", ps and ps.video_url == nil and ps.is_playing == false and tonumber(ps.position_seconds) == 0, tostring(ps))

-- voice_state: the reconnect_attempts NOT-NULL gotcha (confirmed via live
-- `\d tunestream_voice_state`: reconnect_attempts is NOT NULL, no default)
local ok12, err12 = pcall(A.mark_voice_connected, TEST_GUILD, "222")
check("mark_voice_connected (reconnect_attempts NOT NULL zero-fill)", ok12, err12)
local vs = A.q1("SELECT connected_channel_id::text AS connected_channel_id, desired_connected, reconnect_attempts FROM tunestream_voice_state WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("voice_state connected row correct", vs and vs.connected_channel_id == "222" and vs.desired_connected == true and tonumber(vs.reconnect_attempts) == 0, tostring(vs))
local ok13, err13 = pcall(A.mark_voice_disconnected, TEST_GUILD)
check("mark_voice_disconnected runs", ok13, err13)
vs = A.q1("SELECT connected_channel_id, desired_connected FROM tunestream_voice_state WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("voice_state disconnected row correct", vs and vs.connected_channel_id == nil and vs.desired_connected == false, tostring(vs))

-- heartbeat_tick / tunestream_metrics (the %s-placeholder-count vs args-supplied
-- gotcha other sibling bots hit: 8 %s placeholders in the VALUES clause must
-- line up 1:1 with the 8 args passed to q(), or values shift into the wrong
-- column -- e.g. a duration landing in position_seconds).
-- start_time set relative to "now" (not 0) so current_position's elapsed-time
-- math models a track actually 55s into playback, rather than immediately
-- hitting current_position's duration clamp (200s) -- see the port report's
-- note on the current_position bugfix this test's first run surfaced.
local socket_now = require("socket").gettime()
A.playback[TEST_GUILD] = {
  channel_id = "111", url = "https://example.com/a", title = "Track A",
  duration = 200, start_time = socket_now - 55, offset = 0, paused = false, track_uid = uid1,
}
A.enqueue_track(TEST_GUILD, "https://example.com/c", "Track C", TEST_USER) -- so queue_count > 0
local ok15, err15 = pcall(A.heartbeat_tick)
check("heartbeat_tick runs with an active player", ok15, err15)
local metrics = A.q1("SELECT queue_count, is_playing_db, is_paused_db, position_seconds, duration_seconds FROM tunestream_metrics WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("tunestream_metrics row written", metrics ~= nil, tostring(metrics))
check("tunestream_metrics is_paused_db is boolean false, not a position/duration value", metrics and metrics.is_paused_db == false, metrics and tostring(metrics.is_paused_db))
check("tunestream_metrics position_seconds is a plausible position (current_position, not duration/title)", metrics and tonumber(metrics.position_seconds) ~= nil and tonumber(metrics.position_seconds) < 300, metrics and tostring(metrics.position_seconds))
check("tunestream_metrics duration_seconds == 200 (not swapped with position)", metrics and tonumber(metrics.duration_seconds) == 200, metrics and tostring(metrics.duration_seconds))

-- persist_positions_tick (the 10s position-checkpoint write path)
local ok16, err16 = pcall(A.persist_positions_tick)
check("persist_positions_tick runs with an active player", ok16, err16)
local pst = A.q1("SELECT position_seconds FROM tunestream_playback_state WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
check("persist_positions_tick wrote a position (~55s)", pst and tonumber(pst.position_seconds) == 55, pst and tostring(pst.position_seconds))
A.playback[TEST_GUILD] = nil

-- error_events (report_error) -- the gotcha #6 fix: rhythm.lua's port left
-- ERROR_WEBHOOK_URL parsed but never wired to a table write.
local ok14, err14 = pcall(A.report_error, TEST_GUILD, "smoke_test", "smoke test error", "this is a test error event, safe to ignore")
check("report_error / persist_error_event runs", ok14, err14)
local ee = A.q1("SELECT title, error_type FROM tunestream_error_events WHERE guild_id = %s AND error_type = 'smoke_test' ORDER BY created_at DESC LIMIT 1", TEST_GUILD)
check("error_events row persisted", ee and ee.title == "smoke test error", tostring(ee))

-- cleanup: remove every row this test wrote, and ONLY rows scoped to
-- TEST_GUILD/TEST_USER* -- never touch real guild_id rows (verified below).
A.q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
A.q("DELETE FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
A.q("DELETE FROM tunestream_playback_state WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
A.q("DELETE FROM tunestream_guild_settings WHERE guild_id = %s", TEST_GUILD)
A.q("DELETE FROM tunestream_swarm_toggles WHERE guild_id = %s", TEST_GUILD)
A.q("DELETE FROM tunestream_track_intelligence WHERE guild_id = %s", TEST_GUILD)
A.q("DELETE FROM tunestream_user_track_affinity WHERE guild_id = %s", TEST_GUILD)
A.q("DELETE FROM tunestream_voice_state WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)
A.q("DELETE FROM tunestream_error_events WHERE guild_id = %s", TEST_GUILD)
A.q("DELETE FROM tunestream_metrics WHERE guild_id = %s AND bot_name = 'tunestream'", TEST_GUILD)

local leftover = A.q1([[
  SELECT
    (SELECT COUNT(*) FROM tunestream_queue WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_queue_backup WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_playback_state WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_guild_settings WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_track_intelligence WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_user_track_affinity WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_voice_state WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_error_events WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM tunestream_metrics WHERE guild_id = %s) AS n
]], TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD)
check("cleanup left zero test rows behind", leftover and tonumber(leftover.n) == 0, leftover and tostring(leftover.n))

-- Guard rail: confirm the one real pre-existing row (swarm health monitoring
-- for the live guild) is untouched by this test run.
local real_guild = A.q1("SELECT is_playing_db FROM tunestream_metrics WHERE guild_id = %s AND bot_name = 'tunestream'", "1247394560007471134")
check("pre-existing real guild_id 1247394560007471134 row in tunestream_metrics untouched", real_guild ~= nil, tostring(real_guild))

print(string.format("\n%d passed, %d failed", pass, fail))
if fail > 0 then os.exit(1) end
