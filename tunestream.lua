--[[
  TUNESTREAM -- swarm music node.
  LuaJIT port of tunestream.py, node `tunestream` of the 13-bot swarm.

  tunestream.py is byte-for-byte identical to rhythm.py, alucard.py, maestro.py,
  and melodic.py aside from the bot-name/table-prefix/env-prefix substitution
  (confirmed by diffing tunestream.py against melodic.py during this port: after
  normalizing both bot names to a common placeholder, the only remaining diff
  hunks are the BOT_STAGGER_SLOTS dict's per-bot slot numbers and a handful of
  incidental blank-line/whitespace differences -- zero residual logic diffs) --
  the same codebase reskinned under different names/branding. Per the
  orchestrator's guidance, this file was therefore built from
  discord_music_bot_melodic/melodic.lua (the most recently-built/up-to-date
  reference at the time of this port, itself built from maestro.lua, itself
  from alucard.lua -- see melodic.lua's own header for that lineage and its
  current_position() clamping/timescale bugfix vs rhythm.lua) via a mechanical
  melodic->tunestream rename (case-sensitive sed pass, verified zero residual
  "melodic" hits afterward), then re-diffed line-by-line against tunestream.py
  to confirm behavior parity.

  Full command-surface parity with tunestream.py: all 59 of its
  `/tunestream_main_*` slash commands are ported (verified name-for-name against
  tunestream.py's @bot.tree.command declarations -- see the diff in the port
  report). tunestream_error_events IS ported here (same as alucard.lua; unlike
  rhythm.lua, which left ERROR_WEBHOOK_URL parsed but unused/dead) --
  command/button-handler failures, process_queue errors, and maintenance-loop
  errors all land in the shared tunestream_error_events table (same table
  Aria/SwarmPanel reads from for the cross-bot error dashboard) and, if
  TUNESTREAM_ERROR_WEBHOOK_URL is configured, post to a dedicated error webhook,
  matching tunestream.py's _persist_error_event()/send_error_webhook_log() pair.
  TUNESTREAM_WEBHOOK_URL (operational "node online" style logging) is also
  wired, matching tunestream.py's send_webhook_log().

  Intentionally NOT ported (matching rhythm.lua's, alucard.lua's, and
  nexus.lua's documented precedent for this same swarm-wide Python codebase;
  tunestream.py's own source also carries an alucard_status_messages-style
  in-place status-message-edit feature (tunestream_status_messages) that
  none of the 10 already-completed sibling ports carried over either --
  dropped here too for the same reason: the feedback-channel "now playing"
  post already covers the same operational need without a message-ID
  tracking table):
    - Redis cross-process locks, local ffmpeg/aubio audio cache + BPM/loudness
      analysis, YouTube Data API playlist fallback -- asyncio/aiomysql-races
      era plumbing that doesn't apply to this single-process LuaJIT stack.
    - The DAVE-protocol aiohttp monkeypatch (tunestream.py's
      patched_request/patched__request, working around a Lavalink 4.2.2 E2EE
      voice-channelId bug specific to wavelink's aiohttp transport). This
      LuaJIT stack's swarmlua/lavalink.lua sends its own PATCH payloads
      directly and does not implement DAVE/E2EE opus encryption, so the bug
      this patch worked around does not exist here.
    - Gemini track embeddings/cooccurrence (tunestream_track_embeddings,
      tunestream_track_cooccurrence) -- Auto-DJ below is a simplified
      scoring model (see "Smart Auto-DJ" section) driven by
      tunestream_track_intelligence / tunestream_user_track_affinity counters
      instead of semantic embedding similarity.
    - The Aria/SwarmPanel remote-control bridge is ported: see
      poll_swarm_overrides (tunestream_swarm_overrides) and
      poll_direct_orders/handle_direct_order (tunestream_swarm_direct_orders).
    - "Live playlist sync" (tunestream_active_playlist_tracks, the background
      task that re-scans a queued playlist URL for newly-added videos) --
      /tunestream_main_play expands a playlist once, in full, at queue time via
      Lavalink's loadType=playlist response. tunestream_active_playlists (the
      per-guild "what playlist is active" row, not the per-track sync table)
      IS still ported/used the same way rhythm.lua uses it.
    - "Beat-matched mixing" (/tunestream_main_fade mix) falls back to the same
      smooth-fade curve as /tunestream_main_fade fade -- no BPM analysis/matching
      was ported.
    - Vote-skip/upvote/downvote quorums use a fixed threshold of 2 rather than
      a proportional "listeners in channel" quorum, since this file does not
      maintain a full per-channel voice member roster.
    - The dozen-plus overlapping @tasks.loop coroutines (position updater,
      metrics heartbeat, cache eviction/reconcile, zombie reaper, database
      janitor, queue shuffle maintenance, periodic restart, queue integrity
      check, auto-heal, etc.) collapse into the small set of copas background
      threads in start_background_loops()/maintenance_tick() below: position
      persistence, swarm heartbeat + tunestream_metrics, and error-event
      retention. This LuaJIT stack is single-process/blocking-REST, so most
      of those dozen loops existed only to paper over asyncio/aiomysql-pool
      races that don't apply here.
]]

package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

local copas = require("copas")
local cjson = require("cjson")
local https = require("ssl.https")
local ltn12 = require("ltn12")
local socket = require("socket")

local Bot = require("swarmlua.bot")

math.randomseed(os.time() + (socket.gettime() * 1000) % 1000000)

-- ===========================================================================
-- Env / configuration (mirrors tunestream.py's BOT_ENV_PREFIX = "TUNESTREAM")
-- ===========================================================================

local function env(name, default)
  local v = os.getenv(name)
  if v == nil or v == "" then return default end
  return v
end

local PREFIX = "TUNESTREAM"
local TOKEN = env(PREFIX .. "_DISCORD_TOKEN", "")
if TOKEN == "" then
  io.stderr:write("[tunestream] " .. PREFIX .. "_DISCORD_TOKEN is not set; refusing to start.\n")
  os.exit(1)
end

local DB_CONFIG = {
  host = env(PREFIX .. "_DB_HOST", env("DB_HOST", "127.0.0.1")),
  port = tonumber(env(PREFIX .. "_DB_PORT", env("DB_PORT", "5432"))),
  user = env(PREFIX .. "_DB_USER", env("DB_USER", "botuser")),
  password = env(PREFIX .. "_DB_PASSWORD", env("DB_PASSWORD", "")),
  database = env(PREFIX .. "_DB_NAME", "discord_music_tunestream"),
}

local LAVALINK_URI = env(PREFIX .. "_LAVALINK_URI", env(PREFIX .. "_LAVALINK_URL", env("LAVALINK_URI", env("LAVALINK_URL", "http://127.0.0.1:2333"))))
local LAVALINK_PASSWORD = env(PREFIX .. "_LAVALINK_PASSWORD", env("LAVALINK_PASSWORD", ""))
if LAVALINK_PASSWORD == "" then
  io.stderr:write("[tunestream] Set " .. PREFIX .. "_LAVALINK_PASSWORD or LAVALINK_PASSWORD before starting tunestream.\n")
  os.exit(1)
end

-- Parse host/port out of LAVALINK_URI (accepts "host:port", "http://host:port", etc).
local function parse_lavalink(uri)
  local u = tostring(uri or ""):gsub("^%a+://", "")
  local host, port = u:match("^([^:/]+):?(%d*)")
  host = host or "127.0.0.1"
  port = tonumber(port) or 2333
  return host, port
end
local LL_HOST, LL_PORT = parse_lavalink(LAVALINK_URI)

local WEBHOOK_URL = env(PREFIX .. "_WEBHOOK_URL", "")
local ERROR_WEBHOOK_URL = env(PREFIX .. "_ERROR_WEBHOOK_URL", env("SWARM_ERROR_WEBHOOK_URL", env("ERROR_WEBHOOK_URL", "")))
local LOG_DIR = env(PREFIX .. "_LOG_DIR", env("MUSIC_BOT_LOG_DIR", "/app/logs"))
pcall(function() os.execute("mkdir -p '" .. LOG_DIR .. "'") end)
local LOG_FILE = LOG_DIR .. "/tunestream.log"

-- Matches alucard.py's RotatingFileHandler (10MB x 5 backups by default,
-- same env var names) -- LOG_FILE previously grew unbounded forever.
local LOG_MAX_BYTES = tonumber(env("MUSIC_BOT_LOG_MAX_BYTES", "10485760")) or 10485760
local LOG_BACKUP_COUNT = tonumber(env("MUSIC_BOT_LOG_BACKUP_COUNT", "5")) or 5

local function rotate_log_if_needed()
  local f = io.open(LOG_FILE, "r")
  if not f then return end
  local size = f:seek("end")
  f:close()
  if not size or size < LOG_MAX_BYTES then return end
  for i = LOG_BACKUP_COUNT - 1, 1, -1 do
    os.rename(LOG_FILE .. "." .. i, LOG_FILE .. "." .. (i + 1))
  end
  os.rename(LOG_FILE, LOG_FILE .. ".1")
end

local function logline(level, fmt, ...)
  local ok, msg = pcall(string.format, fmt, ...)
  if not ok then msg = fmt end
  local line = string.format("%s %-5s tunestream %s", os.date("%Y-%m-%d %H:%M:%S"), level, msg)
  print(line)
  rotate_log_if_needed()
  local f = io.open(LOG_FILE, "a")
  if f then f:write(line .. "\n"); f:close() end
end
local function log_info(fmt, ...) logline("INFO", fmt, ...) end
local function log_warn(fmt, ...) logline("WARN", fmt, ...) end
local function log_error(fmt, ...) logline("ERROR", fmt, ...) end

-- ===========================================================================
-- Small utilities
-- ===========================================================================

local function trim(s)
  if s == nil then return nil end
  return (tostring(s):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function is_blank(s) return s == nil or trim(s) == "" end

local function new_uid(len)
  len = len or 32
  local chars = "0123456789abcdef"
  local out = {}
  for i = 1, len do
    local idx = math.random(1, #chars)
    out[i] = chars:sub(idx, idx)
  end
  return table.concat(out)
end

local function track_key(url, title)
  local basis = (not is_blank(url)) and url or (title or "")
  basis = trim(basis):lower()
  if #basis > 64 then basis = basis:sub(1, 64) end
  return basis
end

local function clamp(v, lo, hi) return math.max(lo, math.min(hi, v)) end

local function truthy_env(name, default)
  local v = os.getenv(name)
  if v == nil then return default end
  v = v:lower()
  return not (v == "0" or v == "false" or v == "off" or v == "no")
end

-- HTTP GET helper for third-party APIs (LRCLIB, YouTube suggest) -- blocking,
-- same trade-off documented in lua-shared/README.md for rest.lua.
local function http_get_json(url)
  local response_body = {}
  local ok, status = https.request({
    url = url,
    method = "GET",
    headers = { ["User-Agent"] = "swarmlua-tunestream/1.0" },
    sink = ltn12.sink.table(response_body),
  })
  if not ok or (type(status) == "number" and status >= 400) then return nil end
  local raw = table.concat(response_body)
  if #raw == 0 then return nil end
  local ok2, decoded = pcall(cjson.decode, raw)
  if not ok2 then return nil end
  return decoded
end

local function urlencode(s)
  return (tostring(s):gsub("[^%w%-%.%_%~]", function(c)
    return string.format("%%%02X", string.byte(c))
  end))
end

-- ===========================================================================
-- Bot instance
-- ===========================================================================

-- GUILDS(1) + GUILD_VOICE_STATES(1<<7) -- narrowed from swarmlua's broad
-- default per the README's guidance; tunestream needs neither message content
-- nor guild-member-list intents for slash commands + voice/Lavalink.
local INTENTS = 1 + 128

local bot = Bot.new({
  token = TOKEN,
  intents = INTENTS,
  db = DB_CONFIG,
  lavalink = { host = LL_HOST, port = LL_PORT, password = LAVALINK_PASSWORD },
})

-- ===========================================================================
-- DB helpers
-- ===========================================================================

local function q(sql, ...)
  local rows, err = bot.db:query(sql, ...)
  if not rows then
    log_error("db query failed: %s | sql=%s", tostring(err), sql:sub(1, 200))
    return {}
  end
  return rows
end

local function q1(sql, ...)
  local rows = q(sql, ...)
  return rows[1]
end

-- character(n) columns come back space-padded from Postgres via some drivers;
-- pgmoon actually returns them already correctly typed, but trim defensively.
local function col(row, name)
  if not row then return nil end
  local v = row[name]
  if type(v) == "string" then return trim(v) end
  return v
end

local function truncate(value, limit)
  local text = tostring(value or "")
  if #text <= limit then return text end
  return text:sub(1, limit - 3) .. "..."
end

-- ===========================================================================
-- Schema bootstrap (tables already exist / were migrated -- this only adds
-- indexes/columns defensively and is safe to run every boot).
-- ===========================================================================

local function init_db()
  q([[INSERT INTO tunestream_guild_settings (guild_id, volume, loop_mode, filter_mode, transition_mode,
        fade_seconds, fade_curve, custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc)
      VALUES (%s, 100, 'queue', 'none', 'off', 3.0, 'smooth', 1.0, 1.0, 0, false, false)
      ON CONFLICT (guild_id) DO NOTHING]], 0)
  -- the above is a harmless no-op probe (guild_id=0) just to confirm the table/columns are reachable
  q("DELETE FROM tunestream_guild_settings WHERE guild_id = %s", 0)
  -- Live playlist sync (see playlist_sync_tick() below) -- these tables
  -- existed in name only before (this Lua rewrite never had its own
  -- schema-bootstrap pass for them).
  q([[CREATE TABLE IF NOT EXISTS tunestream_active_playlists (
        guild_id BIGINT, bot_name VARCHAR(50) DEFAULT 'tunestream', playlist_url TEXT,
        known_track_count INT DEFAULT 0, requester_id BIGINT, channel_id BIGINT,
        PRIMARY KEY (guild_id, bot_name))]])
  q([[CREATE TABLE IF NOT EXISTS tunestream_active_playlist_tracks (
        guild_id BIGINT, bot_name VARCHAR(50) DEFAULT 'tunestream', playlist_url TEXT,
        position_idx INT DEFAULT 0, track_key VARCHAR(64), track_uid VARCHAR(32),
        video_url TEXT, title TEXT, requester_id BIGINT)]])
  q("CREATE INDEX IF NOT EXISTS tunestream_active_playlist_tracks_lookup_idx ON tunestream_active_playlist_tracks (guild_id, bot_name)")
  log_info("database reachable, schema looks good")
end

local function ensure_guild_settings(guild_id)
  q([[INSERT INTO tunestream_guild_settings (guild_id, volume, loop_mode, filter_mode, transition_mode,
        fade_seconds, fade_curve, custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc)
      VALUES (%s, 100, 'queue', 'none', 'off', 3.0, 'smooth', 1.0, 1.0, 0, false, false)
      ON CONFLICT (guild_id) DO NOTHING]], guild_id)
end

local DEFAULT_SETTINGS = {
  home_vc_id = nil, volume = 100, loop_mode = "queue", filter_mode = "none",
  dj_role_id = nil, feedback_channel_id = nil, transition_mode = "off",
  fade_seconds = 3.0, fade_curve = "smooth", custom_speed = 1.0, custom_pitch = 1.0,
  custom_modifiers_left = 0, dj_only_mode = false, stay_in_vc = false, filter_stack = nil,
}

local function get_settings(guild_id)
  ensure_guild_settings(guild_id)
  local row = q1("SELECT * FROM tunestream_guild_settings WHERE guild_id = %s", guild_id)
  if not row then return DEFAULT_SETTINGS end
  local out = {}
  for k, v in pairs(DEFAULT_SETTINGS) do out[k] = row[k]; if out[k] == nil then out[k] = v end end
  return out
end

local function get_home_channel_id(guild_id)
  local row = q1("SELECT home_vc_id FROM tunestream_bot_home_channels WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  return row and row.home_vc_id or nil
end

local function get_autodj_enabled(guild_id)
  local row = q1("SELECT auto_dj FROM tunestream_swarm_toggles WHERE guild_id = %s", guild_id)
  if row and row.auto_dj ~= nil then return row.auto_dj end
  return false
end

local function set_autodj_enabled(guild_id, enabled)
  q([[INSERT INTO tunestream_swarm_toggles (guild_id, auto_dj) VALUES (%s, %s)
      ON CONFLICT (guild_id) DO UPDATE SET auto_dj = EXCLUDED.auto_dj]], guild_id, enabled)
end

-- ===========================================================================
-- Discord embed / interaction reply helpers
-- (Bot:reply only does plain content; tunestream's whole personality is embeds,
--  so we talk to bot.rest directly here -- same pattern the README shows.)
-- ===========================================================================

local COLOR = {
  green = 0x2ECC71, red = 0xE74C3C, blue = 0x3498DB, blurple = 0x5865F2,
  orange = 0xE67E22, gold = 0xF1C40F, purple = 0x9B59B6, dark = 0x2B2D31,
}

local function embed(desc, opts)
  opts = opts or {}
  local e = { description = desc, color = opts.color or COLOR.blurple }
  if opts.title then e.title = opts.title end
  if opts.footer then e.footer = { text = opts.footer } end
  if opts.fields then e.fields = opts.fields end
  if opts.author then e.author = { name = opts.author } end
  return e
end

local function INT_CALLBACK(itype, data)
  return { type = itype, data = data }
end

local function ack(interaction, content_or_embed, ephemeral, is_embed)
  local data = { flags = ephemeral and 64 or nil }
  if is_embed then data.embeds = { content_or_embed } else data.content = content_or_embed end
  bot.rest:post(("/interactions/%s/%s/callback"):format(interaction.id, interaction.token), INT_CALLBACK(4, data))
end

local function ack_embed(interaction, e, ephemeral) ack(interaction, e, ephemeral, true) end

local function defer(interaction, ephemeral)
  bot.rest:post(("/interactions/%s/%s/callback"):format(interaction.id, interaction.token),
    INT_CALLBACK(5, { flags = ephemeral and 64 or nil }))
end

local function followup(interaction, payload)
  return bot.rest:post(("/webhooks/%s/%s"):format(bot.application_id, interaction.token), payload)
end

local function followup_embed(interaction, e, ephemeral)
  return followup(interaction, { embeds = { e }, flags = ephemeral and 64 or nil })
end

local function followup_content(interaction, content, ephemeral)
  return followup(interaction, { content = content, flags = ephemeral and 64 or nil })
end

-- Component (button) interaction ack that redraws the message in place.
local function update_message(interaction, e, components)
  local data = { embeds = { e } }
  if components then data.components = components end
  bot.rest:post(("/interactions/%s/%s/callback"):format(interaction.id, interaction.token), INT_CALLBACK(7, data))
end

local function autocomplete_result(interaction, choices)
  bot.rest:post(("/interactions/%s/%s/callback"):format(interaction.id, interaction.token),
    INT_CALLBACK(8, { choices = choices }))
end

-- Reads a top-level slash-command option by name.
local function get_opt(interaction, name)
  local opts = interaction.data and interaction.data.options
  if not opts then return nil end
  for _, o in ipairs(opts) do
    if o.name == name then return o.value, o end
  end
  return nil
end

local function focused_opt(interaction)
  local opts = interaction.data and interaction.data.options
  if not opts then return nil end
  for _, o in ipairs(opts) do
    if o.focused then return o end
  end
  return nil
end

local function guild_id_of(interaction) return interaction.guild_id end

local function member_display_name(member)
  if not member then return "Someone" end
  if member.nick and member.nick ~= cjson.null and member.nick ~= "" then return member.nick end
  local u = member.user or {}
  return u.global_name or u.username or "Someone"
end

-- Best-effort member lookup for building requester names in queue/history lists.
local requester_name_cache = {}
local function resolve_requester_name(guild_id, user_id)
  if not user_id then return "Unknown" end
  local cache_key = tostring(user_id)
  local cached = requester_name_cache[cache_key]
  if cached and (socket.gettime() - cached.at) < 900 then return cached.name end
  local member = bot.rest:get(("/guilds/%s/members/%s"):format(guild_id, user_id))
  local name = "Unknown User"
  if member then name = member_display_name(member) end
  requester_name_cache[cache_key] = { name = name, at = socket.gettime() }
  return name
end

-- ===========================================================================
-- Operational webhook + error events: mirrors tunestream.py's send_webhook_log()/
-- _persist_error_event()/send_error_webhook_log() -- writes into the shared
-- tunestream_error_events table (same table Aria/SwarmPanel reads from for the
-- cross-bot error dashboard) and, if configured, posts to a dedicated error
-- webhook. Ported the same way nexus.lua/gws.lua/harmonic.lua added this;
-- rhythm.lua's port (this file's structural template) parsed
-- RHYTHM_ERROR_WEBHOOK_URL but left it dead -- fixed here per the port report.
-- ===========================================================================

local function send_webhook_log(title, description, color, username)
  if WEBHOOK_URL == "" or WEBHOOK_URL == "PASTE_YOUR_NEW_WEBHOOK_URL_HERE" then return end
  local ok, body = pcall(function()
    local response_body = {}
    local payload = cjson.encode({
      username = username or "Node: Tunestream",
      embeds = { { title = title, description = description, color = color or COLOR.blurple, footer = { text = "Swarm Network Matrix" } } },
    })
    https.request({
      url = WEBHOOK_URL, method = "POST",
      headers = { ["Content-Type"] = "application/json", ["Content-Length"] = tostring(#payload) },
      source = ltn12.source.string(payload), sink = ltn12.sink.table(response_body),
    })
  end)
  if not ok then log_warn("webhook log failed: %s", tostring(body)) end
end

local function persist_error_event(guild_id, level, error_type, title, description)
  q([[INSERT INTO tunestream_error_events (bot_name, guild_id, error_level, error_type, title, description)
      VALUES ('tunestream', %s, %s, %s, %s, %s)]],
    guild_id, level, error_type, truncate(title, 255), truncate(description, 5000))
end

local function send_error_webhook(title, description)
  if ERROR_WEBHOOK_URL == "" then return end
  local ok, body = pcall(function()
    local response_body = {}
    local payload = cjson.encode({
      embeds = { { title = "\xF0\x9F\x94\xB4 Tunestream Error: " .. truncate(title, 200), description = truncate(description, 4000),
                   color = COLOR.red, footer = { text = "Tunestream Music Bot" } } },
    })
    https.request({
      url = ERROR_WEBHOOK_URL, method = "POST",
      headers = { ["Content-Type"] = "application/json", ["Content-Length"] = tostring(#payload) },
      source = ltn12.source.string(payload), sink = ltn12.sink.table(response_body),
    })
  end)
  if not ok then log_warn("error webhook failed: %s", tostring(body)) end
end

-- guild_id may be nil (process-wide failures, e.g. the maintenance loop).
local function report_error(guild_id, error_type, title, description)
  local ok, err = pcall(persist_error_event, guild_id, "error", error_type, title, description)
  if not ok then log_warn("failed to persist error event: %s", tostring(err)) end
  send_error_webhook(title, description)
end

-- Wired into swarmlua.bot's additive/optional on_error hook (see
-- lib/swarmlua/bot.lua) so command handler failures also land in
-- tunestream_error_events / the error webhook, not just process_queue/
-- maintenance below. bot.lua's on_error hook only covers type=2
-- (APPLICATION_COMMAND) interaction failures -- the autocomplete/panel-button
-- MESSAGE_COMPONENT routing further down wraps its own pcall and calls
-- report_error directly, since those never reach bot:run()'s dispatcher.
bot.on_error = function(err, context)
  report_error(nil, "command_error", tostring(context and context.command or "unknown"), tostring(err))
end

-- ===========================================================================
-- DJ / permission checks
-- ===========================================================================

local ADMINISTRATOR = 0x8

local guild_owner_cache = {} -- guild_id -> owner user_id

local function get_guild_owner_id(guild_id)
  if not guild_id then return nil end
  local cached = guild_owner_cache[guild_id]
  if cached then return cached end
  local g = bot.rest:get("/guilds/" .. tostring(guild_id))
  local owner_id = g and g.owner_id
  if owner_id then guild_owner_cache[guild_id] = owner_id end
  return owner_id
end

local function has_admin(interaction)
  local member = interaction.member
  if member and member.permissions then
    local perms = tonumber(member.permissions) or 0
    if perms % (ADMINISTRATOR * 2) >= ADMINISTRATOR then return true end
  end
  -- The guild owner always effectively has admin, but Discord's resolved
  -- member.permissions bitfield on the interaction has been observed to not
  -- reliably reflect that -- fall back to an explicit ownership check
  -- rather than ever locking the actual owner out of admin-gated commands.
  local uid = member and member.user and member.user.id
  local owner_id = uid and get_guild_owner_id(interaction.guild_id)
  return owner_id ~= nil and tostring(owner_id) == tostring(uid)
end

local function member_has_role(member, role_id)
  if not member or not member.roles or not role_id then return false end
  for _, r in ipairs(member.roles) do
    if tostring(r) == tostring(role_id) then return true end
  end
  return false
end

-- Returns true/false. On false, sends the "you need the DJ role" reply itself
-- (matching python's is_dj(interaction) two-in-one behavior) unless silent.
local function is_dj(interaction, silent)
  if has_admin(interaction) then return true end
  local gid = guild_id_of(interaction)
  local settings = get_settings(gid)
  if settings.dj_only_mode then
    if settings.dj_role_id and member_has_role(interaction.member, settings.dj_role_id) then return true end
    if not silent then
      ack_embed(interaction, embed("**Strict DJ Mode is Active.** You need the DJ Role.", { color = COLOR.red }), true)
    end
    return false
  end
  return true
end

-- ===========================================================================
-- Playback runtime state (in-memory, per guild)
-- ===========================================================================

-- playback[guild_id] = {
--   url, title, duration (s), start_time (socket.gettime), offset (s),
--   requester_id, track_uid, paused, filter_mode, filter_stack, channel_id,
--   volume, speed, transition_mode
-- }
local playback = {}
local process_queue_busy = {}   -- guild_id -> true while process_queue is running (avoid re-entrancy)
local vote_skip_sessions = {}   -- guild_id -> {[user_id]=true}
local queue_upvotes = {}        -- "guild:queue_id" -> {[user_id]=true}
local queue_downvotes = {}
local sleep_timers = {}         -- guild_id -> {cancel=false}
local fade_tokens = {}          -- guild_id -> incrementing token; a running fade checks it's still current

-- BUGFIX vs rhythm.lua (found live via db_smoke_test.lua's heartbeat_tick/
-- persist_positions_tick checks): the un-clamped version returned
-- data.offset + elapsed with no ceiling, so a stale/zero start_time (or any
-- drift bug elsewhere) produces a nonsensical multi-billion-second position
-- instead of pinning to track length -- and it never multiplied elapsed by
-- the active timescale/nightcore speed, so filtered playback's displayed
-- position and DB-persisted position would drift from the real track
-- position over time. Both fixed here to match nexus.lua's current_position.
local function current_position(guild_id)
  local data = playback[guild_id]
  if not data then return 0 end
  local pos
  if data.paused then
    pos = data.offset or 0
  else
    local elapsed = socket.gettime() - (data.start_time or socket.gettime())
    pos = (data.offset or 0) + elapsed * (data.speed or 1.0)
  end
  if data.duration and data.duration > 0 then pos = math.min(pos, data.duration) end
  return math.floor(math.max(0, pos))
end

local function progress_bar(cur, total, length)
  length = length or 15
  if not total or total <= 0 then
    return string.format("[%s] %d:%02d / Live", string.rep("\xE2\x96\xAC", length), math.floor(cur / 60), cur % 60)
  end
  local progress = clamp(math.floor((cur / total) * length), 0, length)
  local bar = string.rep("\xE2\x96\xAC", progress) .. "\xF0\x9F\x94\x98" .. string.rep("\xE2\x96\xAC", math.max(0, length - progress - 1))
  return string.format("[%s] %d:%02d / %d:%02d", bar, math.floor(cur / 60), cur % 60, math.floor(total / 60), total % 60)
end

-- ===========================================================================
-- Lavalink search / queue helpers
-- ===========================================================================

local function is_explicit_lavalink_query(v)
  v = tostring(v or ""):lower()
  if v:match("^https?://") then return true end
  return v:match("^[a-z0-9_]+search:") ~= nil
end

local function is_playlist_source(v)
  v = tostring(v or "")
  if not v:match("^https?://") then return false end
  if v:match("[?&]list=") then return true end
  local lowered = v:lower()
  return lowered:find("/playlist", 1, true) ~= nil or lowered:find("/sets/", 1, true) ~= nil
end

-- Wraps a Lavalink /v4/loadtracks response into {entries, playlist_name, err}.
-- entries[i] = {uri, title, author, length_ms, encoded}
local function unwrap_load_result(result)
  if not result then return {}, nil, "no response from Lavalink" end
  local load_type = result.loadType
  if load_type == "error" then
    local msg = result.data and result.data.message or "unknown Lavalink error"
    return {}, nil, msg
  end
  if load_type == "empty" then
    return {}, nil, nil
  end
  if load_type == "track" then
    local info = result.data.info
    return { { uri = info.uri, title = info.title, author = info.author, length_ms = info.length, encoded = result.data.encoded } }, nil, nil
  end
  if load_type == "search" then
    local entries = {}
    for _, t in ipairs(result.data or {}) do
      table.insert(entries, { uri = t.info.uri, title = t.info.title, author = t.info.author, length_ms = t.info.length, encoded = t.encoded })
    end
    return entries, nil, nil
  end
  if load_type == "playlist" then
    local entries = {}
    for _, t in ipairs((result.data and result.data.tracks) or {}) do
      table.insert(entries, { uri = t.info.uri, title = t.info.title, author = t.info.author, length_ms = t.info.length, encoded = t.encoded })
    end
    local name = result.data and result.data.info and result.data.info.name
    return entries, name, nil
  end
  return {}, nil, "unrecognized Lavalink loadType: " .. tostring(load_type)
end

-- DB-backed search cache (tunestream_search_cache), 15 minute TTL -- mirrors the
-- Python bot's cache table without the extra in-process TTL cache layer.
local function search_cache_get(cache_key)
  local row = q1("SELECT resolved_uri, title FROM tunestream_search_cache WHERE cache_key = %s AND expires_at > NOW()", cache_key)
  if not row then return nil end
  return row.resolved_uri, row.title
end

local function search_cache_put(cache_key, uri, title)
  q([[INSERT INTO tunestream_search_cache (cache_key, resolved_uri, title, cached_at, expires_at)
      VALUES (%s, %s, %s, NOW(), NOW() + INTERVAL '15 minutes')
      ON CONFLICT (cache_key) DO UPDATE SET resolved_uri = EXCLUDED.resolved_uri,
        title = EXCLUDED.title, cached_at = NOW(), expires_at = NOW() + INTERVAL '15 minutes']],
    cache_key, uri, title)
end

-- search_playables(query) -> entries, playlist_name, err
local function search_playables(query)
  local cleaned = trim(query or "")
  if cleaned == "" then return {}, nil, nil end
  if not is_explicit_lavalink_query(cleaned) then
    cleaned = "ytmsearch:" .. cleaned
  end
  local cacheable = not cleaned:match("^https?://") and not cleaned:find("list=", 1, true)
  local cache_key = cleaned:lower()
  if cacheable then
    local cached_uri = search_cache_get(cache_key)
    if cached_uri then
      local result = bot.lavalink:load_tracks(cached_uri)
      local entries, playlist_name, err = unwrap_load_result(result)
      if #entries > 0 then return entries, playlist_name, nil end
    end
  end
  local result, err = bot.lavalink:load_tracks(cleaned)
  if not result then return {}, nil, err or "Lavalink is not reachable" end
  local entries, playlist_name, lerr = unwrap_load_result(result)
  if lerr then return {}, nil, lerr end
  if cacheable and #entries > 0 then
    search_cache_put(cache_key, entries[1].uri, entries[1].title)
  end
  return entries, playlist_name, nil
end

-- ===========================================================================
-- Queue table helpers (tunestream_queue / tunestream_queue_backup)
-- ===========================================================================

local function queue_count(guild_id)
  local row = q1("SELECT COUNT(*)::int AS c FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  return row and row.c or 0
end

local function snapshot_backup(guild_id)
  q("DELETE FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  q([[INSERT INTO tunestream_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid)
      SELECT guild_id, bot_name, video_url, title, requester_id, track_uid FROM tunestream_queue
      WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC]], guild_id)
end

local function enqueue_track(guild_id, url, title, requester_id, track_uid)
  track_uid = track_uid or new_uid()
  q([[INSERT INTO tunestream_queue (guild_id, bot_name, video_url, title, requester_id, track_uid)
      VALUES (%s, 'tunestream', %s, %s, %s, %s)]], guild_id, url, title, requester_id, track_uid)
  -- track_intelligence: queued_count bump (best-effort learning signal for Auto-DJ)
  local uk = track_key(url, title)
  q([[INSERT INTO tunestream_track_intelligence (guild_id, url_key, video_url, title, queued_count, play_count,
        finish_count, skip_count, like_count, dislike_count, total_listen_seconds, last_requester_id, first_seen, last_queued, updated_at)
      VALUES (%s, %s, %s, %s, 1, 0, 0, 0, 0, 0, 0, %s, NOW(), NOW(), NOW())
      ON CONFLICT (guild_id, url_key) DO UPDATE SET
        queued_count = tunestream_track_intelligence.queued_count + 1,
        last_requester_id = EXCLUDED.last_requester_id, last_queued = NOW(), updated_at = NOW()]],
    guild_id, uk, url, title, requester_id)
  return track_uid
end

local function insert_queue_front(guild_id, url, title, requester_id, track_uid)
  track_uid = track_uid or new_uid()
  -- Shift by rebuilding: cheapest correct way given id is a serial PK we don't want to renumber.
  local rows = q("SELECT id, video_url, title, requester_id, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", guild_id)
  q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  q("INSERT INTO tunestream_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'tunestream', %s, %s, %s, %s)",
    guild_id, url, title, requester_id, track_uid)
  for _, r in ipairs(rows) do
    q("INSERT INTO tunestream_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'tunestream', %s, %s, %s, %s)",
      guild_id, r.video_url, r.title, r.requester_id, col(r, "track_uid") or new_uid())
  end
  return track_uid
end

local function delete_backup_track(guild_id, track_uid, video_url, title)
  if track_uid then
    q("DELETE FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream' AND track_uid = %s", guild_id, track_uid)
  elseif video_url then
    q([[DELETE FROM tunestream_queue_backup WHERE id IN (
          SELECT id FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream' AND video_url = %s LIMIT 1)]],
      guild_id, video_url)
  elseif title then
    q([[DELETE FROM tunestream_queue_backup WHERE id IN (
          SELECT id FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream' AND title = %s LIMIT 1)]],
      guild_id, title)
  end
end

-- Fisher-Yates shuffle of the live queue, keeping the first row fixed
-- (currently-inserted / about-to-play track) when preserve_first is set.
local function shuffle_queue(guild_id, preserve_first)
  local rows = q("SELECT id, video_url, title, requester_id, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", guild_id)
  if #rows <= 1 then return #rows end
  local head = nil
  local body = rows
  if preserve_first then
    head = rows[1]
    body = {}
    for i = 2, #rows do body[#body + 1] = rows[i] end
  end
  for i = #body, 2, -1 do
    local j = math.random(1, i)
    body[i], body[j] = body[j], body[i]
  end
  -- Keep same-title tracks from landing adjacent where possible.
  for i = 2, #body do
    if col(body[i], "title") == col(body[i - 1], "title") then
      for j = i + 1, #body do
        if col(body[j], "title") ~= col(body[i - 1], "title") then
          body[i], body[j] = body[j], body[i]
          break
        end
      end
    end
  end
  q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  local ordered = {}
  if head then ordered[1] = head end
  for _, r in ipairs(body) do ordered[#ordered + 1] = r end
  for _, r in ipairs(ordered) do
    q("INSERT INTO tunestream_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'tunestream', %s, %s, %s, %s)",
      guild_id, r.video_url, r.title, r.requester_id, col(r, "track_uid") or new_uid())
  end
  return #ordered
end

local function restore_queue_from_backup(guild_id)
  local rows = q("SELECT video_url, title, requester_id, track_uid FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", guild_id)
  for _, r in ipairs(rows) do
    q("INSERT INTO tunestream_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'tunestream', %s, %s, %s, %s)",
      guild_id, r.video_url, r.title, r.requester_id, col(r, "track_uid") or new_uid())
  end
  return #rows
end

-- ===========================================================================
-- Live playlist sync: auto-add newly appeared tracks / auto-remove tracks
-- gone from a queued playlist's source. Ported from alucard.py's
-- playlist_sync_loop -- this was cut fleet-wide during the Lua rewrite
-- (tunestream_active_playlists existed only as inert per-guild metadata,
-- written to by nothing but the DELETE calls scattered through /stop,
-- /clear, sleep-timer-elapsed, and queue-drained; is_playlist_source() was
-- defined but never called). Simplified vs. the Python original: no
-- YouTube Data API path (this stack only has Lavalink), so every
-- re-extraction is treated as equally trustworthy and a removal always
-- requires 2 consecutive missing cycles rather than distinguishing an
-- "API-trusted" cycle from a truncating-fallback one.
-- ===========================================================================

local PLAYLIST_SYNC_INTERVAL = tonumber(env("PLAYLIST_SYNC_INTERVAL", "30")) or 30
local PLAYLIST_SYNC_MAX_TRACKED = tonumber(env("PLAYLIST_SYNC_MAX_TRACKED", "500")) or 500
local PLAYLIST_QUEUE_MIN_TRACKS = tonumber(env("PLAYLIST_QUEUE_MIN_TRACKS", "20")) or 20
local playlist_missing_streak = {} -- guild_id -> {track_key -> consecutive cycles missing}

local function clear_active_playlist(guild_id)
  q("DELETE FROM tunestream_active_playlists WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  q("DELETE FROM tunestream_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  playlist_missing_streak[guild_id] = nil
end

-- track_key() itself caps at 64 chars (matches tunestream_track_intelligence's
-- url_key VARCHAR(64)), but tunestream_active_playlist_tracks.track_key is a
-- pre-existing CHAR(40) column -- overflowing it fails the INSERT, and
-- since that happened on every row of every playlist sync, it also tripped
-- the pg.lua reconnect-storm bug fixed above (same failure, same query, in
-- a loop). CHAR (not VARCHAR) also blank-pads on read, so values coming
-- back out of this column need trimming before comparing them against a
-- freshly computed key.
local function playlist_track_key(url, title)
  local k = track_key(url, title)
  if #k > 40 then k = k:sub(1, 40) end
  return k
end

-- Called right after a /play (or equivalent) call resolves to a real
-- Lavalink loadType=playlist response -- registers it so playlist_sync_tick
-- starts watching it for adds/removals.
local function set_active_playlist(guild_id, playlist_url, entries, requester_id, channel_id)
  if not playlist_url or not entries or #entries == 0 then return end
  q([[INSERT INTO tunestream_active_playlists (guild_id, bot_name, playlist_url, known_track_count, requester_id, channel_id)
      VALUES (%s, 'tunestream', %s, %s, %s, %s)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET playlist_url = EXCLUDED.playlist_url,
        known_track_count = EXCLUDED.known_track_count, requester_id = EXCLUDED.requester_id, channel_id = EXCLUDED.channel_id]],
    guild_id, playlist_url, math.min(#entries, PLAYLIST_SYNC_MAX_TRACKED), requester_id, channel_id)
  q("DELETE FROM tunestream_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
  for idx, t in ipairs(entries) do
    if idx > PLAYLIST_SYNC_MAX_TRACKED then break end
    q([[INSERT INTO tunestream_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
        VALUES (%s, 'tunestream', %s, %s, %s, %s, %s, %s, %s)]],
      guild_id, playlist_url, idx - 1, playlist_track_key(t.uri, t.title), new_uid(), t.uri, t.title, requester_id)
  end
  playlist_missing_streak[guild_id] = nil
end

-- One sync cycle across every guild with a registered active playlist:
-- re-extracts each playlist via Lavalink, multiset-diffs it against the
-- last-known snapshot (tunestream_active_playlist_tracks), enqueues additions,
-- and removes tracks that have been missing for 2 consecutive cycles
-- (from both the live queue and its backup mirror) -- plus a loop-mode
-- duplicate trim and a queue-health refill, matching the Python original.
local function playlist_sync_tick()
  local playlists = q("SELECT guild_id, playlist_url, known_track_count, requester_id, channel_id FROM tunestream_active_playlists WHERE bot_name = 'tunestream'") or {}
  for _, prow in ipairs(playlists) do
    local gid = prow.guild_id
    local ok, err = pcall(function()
      local result, lerr = bot.lavalink:load_tracks(prow.playlist_url)
      local entries, _pname, uerr = unwrap_load_result(result)
      if uerr or #entries == 0 then
        log_warn("[%s] playlist sync: re-extract failed: %s", gid, tostring(uerr or lerr))
        return
      end
      if #entries > PLAYLIST_SYNC_MAX_TRACKED then
        local trimmed = {}
        for i = 1, PLAYLIST_SYNC_MAX_TRACKED do trimmed[i] = entries[i] end
        entries = trimmed
      end

      local current_rows = {}
      local current_counts = {}
      for _, t in ipairs(entries) do
        local k = playlist_track_key(t.uri, t.title)
        current_rows[#current_rows + 1] = { url = t.uri, title = t.title, key = k }
        current_counts[k] = (current_counts[k] or 0) + 1
      end

      local previous_rows = q("SELECT track_key, track_uid, video_url, title, requester_id FROM tunestream_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY position_idx ASC", gid) or {}
      local previous_by_key = {}
      local remaining_previous = {}
      for _, p in ipairs(previous_rows) do
        local k = trim(p.track_key or "")
        previous_by_key[k] = previous_by_key[k] or {}
        table.insert(previous_by_key[k], p)
        remaining_previous[k] = (remaining_previous[k] or 0) + 1
      end

      local added_rows = {}
      for _, r in ipairs(current_rows) do
        if remaining_previous[r.key] and remaining_previous[r.key] > 0 then
          remaining_previous[r.key] = remaining_previous[r.key] - 1
        else
          added_rows[#added_rows + 1] = r
        end
      end

      local removed_counts = {}
      local carry_forward = {}
      local streaks = playlist_missing_streak[gid] or {}
      for k, missing_n in pairs(remaining_previous) do
        if missing_n > 0 then
          local streak = (streaks[k] or 0) + 1
          streaks[k] = streak
          if streak >= 2 then
            removed_counts[k] = missing_n
          else
            for i = 1, missing_n do
              local p = previous_by_key[k] and previous_by_key[k][i]
              if p then carry_forward[#carry_forward + 1] = p end
            end
          end
        end
      end
      for k in pairs(current_counts) do streaks[k] = nil end
      playlist_missing_streak[gid] = next(streaks) and streaks or nil

      local purged_live, purged_backup = 0, 0
      if next(removed_counts) ~= nil then
        local budget = {}
        for k, c in pairs(removed_counts) do budget[k] = c end
        local live_rows = q("SELECT id, video_url, title FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", gid) or {}
        for _, lr in ipairs(live_rows) do
          local k = playlist_track_key(lr.video_url, lr.title)
          if budget[k] and budget[k] > 0 then
            budget[k] = budget[k] - 1
            q("DELETE FROM tunestream_queue WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", lr.id, gid)
            purged_live = purged_live + 1
          end
        end
        budget = {}
        for k, c in pairs(removed_counts) do budget[k] = c end
        local backup_rows = q("SELECT id, video_url, title FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", gid) or {}
        for _, br in ipairs(backup_rows) do
          local k = playlist_track_key(br.video_url, br.title)
          if budget[k] and budget[k] > 0 then
            budget[k] = budget[k] - 1
            q("DELETE FROM tunestream_queue_backup WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", br.id, gid)
            purged_backup = purged_backup + 1
          end
        end
      end

      if #added_rows > 0 then
        for _, r in ipairs(added_rows) do enqueue_track(gid, r.url, r.title, prow.requester_id) end
        snapshot_backup(gid)
        if #added_rows > 1 then shuffle_queue(gid, true) end
      end

      -- Trim loop-mode duplicate live-queue copies down to at most 1 per track_key still in the playlist.
      local trim_count = 0
      local current_keys = {}
      for _, r in ipairs(current_rows) do current_keys[r.key] = true end
      local all_rows = q("SELECT id, video_url, title FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", gid) or {}
      local seen = {}
      for _, qr in ipairs(all_rows) do
        local k = playlist_track_key(qr.video_url, qr.title)
        if current_keys[k] then
          if seen[k] then
            q("DELETE FROM tunestream_queue WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", qr.id, gid)
            trim_count = trim_count + 1
          else
            seen[k] = true
          end
        end
      end

      -- Queue-health refill: independent of the diff above, top the live queue back up
      -- if it's thinner than expected relative to the tracked playlist (drained by
      -- playback/a bug/a restart without the source playlist itself changing).
      local refilled = 0
      local queued_rows = q("SELECT video_url, title FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", gid) or {}
      local queued_keys = {}
      local queued_n = 0
      for _, qr in ipairs(queued_rows) do
        local k = playlist_track_key(qr.video_url, qr.title)
        if not queued_keys[k] then queued_keys[k] = true; queued_n = queued_n + 1 end
      end
      local target = math.min(#current_rows, PLAYLIST_QUEUE_MIN_TRACKS)
      if queued_n < target then
        local refill_rows = {}
        for _, r in ipairs(current_rows) do
          if queued_n + #refill_rows >= target then break end
          if not queued_keys[r.key] then refill_rows[#refill_rows + 1] = r end
        end
        for _, r in ipairs(refill_rows) do enqueue_track(gid, r.url, r.title, prow.requester_id) end
        if #refill_rows > 0 then
          refilled = #refill_rows
          snapshot_backup(gid)
          shuffle_queue(gid, true)
        end
      end

      if #added_rows > 0 or purged_live > 0 or purged_backup > 0 or trim_count > 0 or refilled > 0 then
        log_info("[%s] playlist sync: +%d -%d(live) -%d(backup) trimmed=%d refilled=%d", gid, #added_rows, purged_live, purged_backup, trim_count, refilled)
      end

      -- Persist this cycle's confirmed tracks plus anything still in its missing-streak
      -- grace window as the baseline the next cycle diffs against.
      q("DELETE FROM tunestream_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
      local idx = 0
      for _, r in ipairs(current_rows) do
        q([[INSERT INTO tunestream_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
            VALUES (%s, 'tunestream', %s, %s, %s, %s, %s, %s, %s)]],
          gid, prow.playlist_url, idx, r.key, new_uid(), r.url, r.title, prow.requester_id)
        idx = idx + 1
      end
      for _, p in ipairs(carry_forward) do
        q([[INSERT INTO tunestream_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
            VALUES (%s, 'tunestream', %s, %s, %s, %s, %s, %s, %s)]],
          gid, prow.playlist_url, idx, trim(p.track_key or ""), trim(col(p, "track_uid") or "") ~= "" and col(p, "track_uid") or new_uid(), p.video_url, p.title, p.requester_id)
        idx = idx + 1
      end
      q("UPDATE tunestream_active_playlists SET known_track_count = %s WHERE guild_id = %s AND bot_name = 'tunestream'", idx, gid)
    end)
    if not ok then
      log_warn("[%s] playlist sync tick error: %s", gid, tostring(err))
      report_error(gid, "runtime", "playlist sync error", tostring(err))
    end
  end
end

-- ===========================================================================
-- Filter presets (Lavalink v4 /v4/sessions/{id}/players/{guild} filters patch)
-- ===========================================================================

local LOUDNORM_EQ = {
  {0,-0.05},{1,-0.03},{2,0.0},{3,0.0},{4,0.0},{5,0.0},{6,0.0},{7,-0.01},
  {8,-0.02},{9,-0.02},{10,-0.03},{11,-0.02},{12,0.01},{13,0.01},{14,0.0},
}

local function blend_loudnorm(preset_bands)
  local base = {}
  for _, b in ipairs(LOUDNORM_EQ) do base[b[1]] = b[2] end
  for _, b in ipairs(preset_bands) do base[b[1]] = (base[b[1]] or 0) + b[2] end
  local out = {}
  for band = 0, 14 do out[#out + 1] = { band = band, gain = base[band] or 0 } end
  return out
end

local function eq_bands(bands_gain_pairs)
  local out = {}
  for _, b in ipairs(bands_gain_pairs) do out[#out + 1] = { band = b[1], gain = b[2] } end
  return out
end

FILTER_PRESET_CHOICES = {
  { name = "None (Standard high quality audio)", value = "none" },
  { name = "Bassboost", value = "bassboost" },
  { name = "Nightcore", value = "nightcore" },
  { name = "Vaporwave", value = "vaporwave" },
  { name = "8D Rotation", value = "8d" },
  { name = "Karaoke", value = "karaoke" },
  { name = "Tremolo", value = "tremolo" },
  { name = "Vibrato", value = "vibrato" },
  { name = "Low Pass", value = "lowpass" },
  { name = "Lo-fi", value = "lofi" },
  { name = "Electronic", value = "electronic" },
  { name = "Party", value = "party" },
  { name = "Radio", value = "radio" },
  { name = "Cinema", value = "cinema" },
  { name = "Soft", value = "soft" },
  { name = "Pop", value = "pop" },
  { name = "Rock", value = "rock" },
  { name = "Classical", value = "classical" },
  { name = "Ear Rape", value = "earrape" },
  { name = "Double Time (2x)", value = "doubletime" },
  { name = "Slow Mo (0.5x)", value = "slowmo" },
  { name = "Pitch Up", value = "pitch_up" },
  { name = "Pitch Down", value = "pitch_down" },
  { name = "Deep", value = "deep" },
  { name = "Anime", value = "anime" },
}
local FILTER_PRESET_VALUES = {}
for _, c in ipairs(FILTER_PRESET_CHOICES) do FILTER_PRESET_VALUES[c.value] = c.name end

-- apply_filter_preset(filters, mode, current_speed) -> new_speed
-- mutates `filters` (the Lavalink filters payload table) in place.
local function apply_filter_preset(filters, mode, current_speed)
  mode = tostring(mode or "none"):lower():gsub("%s", "")
  local speed = current_speed or 1.0
  local used_eq = false

  if mode == "nightcore" then
    filters.timescale = { speed = 1.25, pitch = 1.3 }; speed = 1.25
  elseif mode == "vaporwave" then
    filters.timescale = { speed = 0.8, pitch = 0.8 }; speed = 0.8
  elseif mode == "bassboost" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,0.32},{1,0.24},{2,0.12} })
  elseif mode == "8d" then
    filters.rotation = { rotationHz = 0.18 }
    filters.channelMix = { leftToLeft = 0.8, leftToRight = 0.2, rightToLeft = 0.2, rightToRight = 0.8 }
  elseif mode == "karaoke" then
    filters.karaoke = { level = 1.0, monoLevel = 1.0, filterBand = 220.0, filterWidth = 100.0 }
  elseif mode == "tremolo" then
    filters.tremolo = { frequency = 4.0, depth = 0.45 }
  elseif mode == "vibrato" then
    filters.vibrato = { frequency = 4.5, depth = 0.35 }
  elseif mode == "lowpass" or mode == "lofi" then
    filters.lowPass = { smoothing = (mode == "lowpass") and 20.0 or 35.0 }
    if mode == "lofi" then filters.timescale = { speed = 0.94, pitch = 0.96 }; speed = 0.94 end
  elseif mode == "electronic" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,0.12},{1,0.10},{4,-0.05},{8,0.08},{10,0.14} })
  elseif mode == "party" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,0.25},{1,0.18},{2,0.08},{9,0.10} })
  elseif mode == "radio" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,-0.18},{1,-0.10},{4,0.12},{5,0.12},{10,-0.12} })
    filters.lowPass = { smoothing = 18.0 }
  elseif mode == "cinema" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,0.18},{1,0.12},{8,0.08},{9,0.10} })
  elseif mode == "soft" then
    filters.lowPass = { smoothing = 25.0 }
  elseif mode == "pop" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,-0.1},{1,0.1},{2,0.2},{3,0.25},{4,0.2},{5,0.1},{6,0.0},{7,-0.05},{8,-0.1},{9,-0.1},{10,-0.05},{11,0.0},{12,0.1},{13,0.2},{14,0.1} })
  elseif mode == "rock" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,0.3},{1,0.25},{2,0.1},{3,-0.1},{4,-0.15},{5,-0.1},{6,0.0},{7,0.1},{8,0.3},{9,0.35},{10,0.3},{11,0.2},{12,0.1},{13,0.05},{14,0.0} })
  elseif mode == "classical" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,0.1},{1,0.1},{2,0.05},{3,0.0},{4,-0.05},{5,-0.05},{6,-0.05},{7,-0.05},{8,-0.05},{9,-0.05},{10,0.0},{11,0.05},{12,0.1},{13,0.15},{14,0.2} })
  elseif mode == "earrape" then
    filters.distortion = { sinOffset = 0.0, sinScale = 1.5, cosOffset = 0.0, cosScale = 1.5, tanOffset = 0.0, tanScale = 1.0, offset = 0.0, scale = 1.5 }
  elseif mode == "doubletime" then
    filters.timescale = { speed = 2.0, pitch = 1.0 }; speed = 2.0
  elseif mode == "slowmo" then
    filters.timescale = { speed = 0.5, pitch = 1.0 }; speed = 0.5
  elseif mode == "pitch_up" then
    filters.timescale = { speed = 1.0, pitch = 1.3 }
  elseif mode == "pitch_down" then
    filters.timescale = { speed = 1.0, pitch = 0.7 }
  elseif mode == "deep" then
    used_eq = true
    filters.equalizer = blend_loudnorm({ {0,0.5},{1,0.4},{2,0.3} })
    filters.timescale = { speed = 1.0, pitch = 0.8 }
  elseif mode == "anime" then
    filters.timescale = { speed = 1.4, pitch = 1.5 }; speed = 1.4
  end

  if not used_eq then
    filters.equalizer = eq_bands(LOUDNORM_EQ)
  end
  return speed
end

-- ===========================================================================
-- Voice connect / queue engine
-- ===========================================================================

local function ensure_voice_connection(guild_id, channel_id)
  bot.gateway:join_voice(guild_id, channel_id, false, false)
  copas.sleep(0.5) -- give VOICE_STATE_UPDATE/VOICE_SERVER_UPDATE a moment to land before we PATCH the player
end

function new_uid_label(uid)
  if not uid then return "------" end
  return uid:sub(1, 8)
end

local function build_now_playing_embed(guild_id, title, url, uploader, track_uid, requester_id)
  local e = embed(nil, { title = "\xF0\x9F\x8E\xB5 Now Playing", color = 0x5865F2 })
  e.description = string.format("**[%s](%s)**\n*By: %s*", title, url, uploader or "Unknown")
  e.fields = { { name = "Track ID", value = "`" .. new_uid_label(track_uid) .. "`", inline = true } }
  if requester_id then
    table.insert(e.fields, { name = "Requested by", value = resolve_requester_name(guild_id, requester_id), inline = true })
  end
  return e
end

-- Best-effort "now playing" post to the guild's configured feedback channel.
local function send_feedback_now_playing(guild_id, title, url, uploader, track_uid, requester_id)
  local settings = get_settings(guild_id)
  if not settings.feedback_channel_id then return end
  local ok = pcall(function()
    bot.rest:post(("/channels/%s/messages"):format(settings.feedback_channel_id),
      { embeds = { build_now_playing_embed(guild_id, title, url, uploader, track_uid, requester_id) } })
  end)
  if not ok then log_warn("[%s] feedback channel post failed", guild_id) end
end

local maybe_enqueue_autodj -- forward decl

-- Port of alucard.py's update_stage_topic/clear_voice_channel_status: without
-- this, a stage channel just sits showing "waiting" in Discord's UI and the
-- member list never shows what's playing, even though audio is flowing fine
-- -- these are purely cosmetic Discord-side signals, not required for audio,
-- but users expect them. Channel type is memoized since it never changes.
local channel_kind_cache = {} -- channel_id -> "stage" | "voice"
local last_stage_topic = {}   -- guild_id -> last topic string sent (dedup)

local function get_channel_kind(channel_id)
  if not channel_id then return "voice" end
  local cached = channel_kind_cache[channel_id]
  if cached then return cached end
  local ch = bot.rest:get("/channels/" .. tostring(channel_id))
  local kind = (ch and ch.type == 13) and "stage" or "voice"
  channel_kind_cache[channel_id] = kind
  return kind
end

local function update_stage_topic(guild_id, channel_id, title)
  if not channel_id then return end
  local ok, err = pcall(function()
    local safe_title = tostring(title or "Unknown Track"):gsub("\n", " "):sub(1, 60)
    local topic = "\xF0\x9F\x8E\xB5 " .. safe_title
    if last_stage_topic[guild_id] == topic then return end
    if get_channel_kind(channel_id) == "stage" then
      local patched = bot.rest:patch("/stage-instances/" .. tostring(channel_id), { topic = topic })
      if not patched then
        bot.rest:post("/stage-instances", { channel_id = tostring(channel_id), topic = topic, privacy_level = 2 })
      end
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = topic })
    end
    last_stage_topic[guild_id] = topic
  end)
  if not ok then log_warn("[%s] stage/voice topic update failed: %s", guild_id, tostring(err)) end
end

local function clear_stage_topic(guild_id, channel_id)
  last_stage_topic[guild_id] = nil
  if not channel_id then return end
  pcall(function()
    if get_channel_kind(channel_id) == "stage" then
      bot.rest:delete("/stage-instances/" .. tostring(channel_id))
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = cjson.null })
    end
  end)
end

-- Pops the next track from tunestream_queue and starts it. If the queue is empty,
-- tries to restore from tunestream_queue_backup (loop_mode == queue), else runs
-- Auto-DJ, else disconnects (unless 24/7 mode is on).
local function process_queue(guild_id, channel_id)
  if process_queue_busy[guild_id] then return end
  process_queue_busy[guild_id] = true
  local ok, err = pcall(function()
    local settings = get_settings(guild_id)
    local next_row = q1("SELECT id, video_url, title, requester_id, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT 1", guild_id)

    if not next_row then
      if settings.loop_mode == "queue" then
        local restored = restore_queue_from_backup(guild_id)
        if restored > 0 then
          shuffle_queue(guild_id, true)
          next_row = q1("SELECT id, video_url, title, requester_id, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT 1", guild_id)
        end
      end
    end

    if not next_row then
      q([[UPDATE tunestream_playback_state SET channel_id = NULL, video_url = NULL, title = NULL,
          position_seconds = 0, is_playing = false, is_paused = false, play_session_key = NULL, track_uid = NULL
          WHERE guild_id = %s AND bot_name = 'tunestream']], guild_id)
      playback[guild_id] = nil
      if maybe_enqueue_autodj(guild_id, channel_id) then return end
      clear_stage_topic(guild_id, channel_id)
      if not settings.stay_in_vc then
        bot.lavalink:destroy_player(guild_id)
        bot.gateway:leave_voice(guild_id)
      end
      return
    end

    local track_uid = col(next_row, "track_uid") or new_uid()
    local url, title, requester_id = next_row.video_url, next_row.title, next_row.requester_id
    q("DELETE FROM tunestream_queue WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", next_row.id, guild_id)

    local result, lerr = bot.lavalink:load_tracks(url)
    local entries, _pn, uerr = unwrap_load_result(result)
    if uerr or #entries == 0 then
      log_warn("[%s] track resolve failed for '%s': %s", guild_id, tostring(title), tostring(uerr or lerr))
      -- Give up on this one track and move to the next rather than getting stuck.
      copas.addthread(function() process_queue(guild_id, channel_id) end)
      return
    end
    local track = entries[1]

    ensure_voice_connection(guild_id, channel_id)

    local filters = {}
    local speed = 1.0
    if settings.custom_modifiers_left and settings.custom_modifiers_left > 0 then
      filters.timescale = { speed = settings.custom_speed, pitch = settings.custom_pitch }
      speed = settings.custom_speed
      q("UPDATE tunestream_guild_settings SET custom_modifiers_left = custom_modifiers_left - 1 WHERE guild_id = %s", guild_id)
      if settings.custom_modifiers_left - 1 <= 0 then
        q("UPDATE tunestream_guild_settings SET custom_speed = 1.0, custom_pitch = 1.0 WHERE guild_id = %s", guild_id)
      end
    else
      speed = apply_filter_preset(filters, settings.filter_mode, speed)
      if settings.filter_stack and settings.filter_stack ~= settings.filter_mode then
        speed = apply_filter_preset(filters, settings.filter_stack, speed)
      end
    end

    local use_fade = settings.transition_mode == "fade" or settings.transition_mode == "smart" or settings.transition_mode == "mix"
    local target_volume = settings.volume

    bot.lavalink:update_player(guild_id, {
      encodedTrack = track.encoded,
      volume = use_fade and 0 or target_volume,
      filters = filters,
    })

    update_stage_topic(guild_id, channel_id, track.title or title)

    playback[guild_id] = {
      url = track.uri or url, title = track.title or title, duration = (track.length_ms or 0) / 1000,
      start_time = socket.gettime(), offset = 0, requester_id = requester_id, track_uid = track_uid,
      paused = false, filter_mode = settings.filter_mode, filter_stack = settings.filter_stack,
      channel_id = channel_id, volume = target_volume, speed = speed, transition_mode = settings.transition_mode,
      original_queue_url = url, original_queue_title = title,
    }

    q([[INSERT INTO tunestream_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, is_paused, title, play_session_key, track_uid)
        VALUES (%s, 'tunestream', %s, %s, 0, true, false, %s, %s, %s)
        ON CONFLICT (guild_id, bot_name) DO UPDATE SET channel_id = EXCLUDED.channel_id, video_url = EXCLUDED.video_url,
          position_seconds = 0, is_playing = true, is_paused = false, title = EXCLUDED.title,
          play_session_key = EXCLUDED.play_session_key, track_uid = EXCLUDED.track_uid, last_checkpoint_at = NOW()]],
      guild_id, channel_id, track.uri or url, track.title or title, track_uid, track_uid)
    q("INSERT INTO tunestream_history (guild_id, video_url, title, requester_id, played_at) VALUES (%s, %s, %s, %s, NOW())",
      guild_id, track.uri or url, track.title or title, requester_id)
    q([[INSERT INTO tunestream_track_intelligence (guild_id, url_key, video_url, title, queued_count, play_count, finish_count, skip_count, like_count, dislike_count, total_listen_seconds, last_requester_id, first_seen, last_played, updated_at)
        VALUES (%s, %s, %s, %s, 0, 1, 0, 0, 0, 0, 0, %s, NOW(), NOW(), NOW())
        ON CONFLICT (guild_id, url_key) DO UPDATE SET play_count = tunestream_track_intelligence.play_count + 1,
          last_requester_id = EXCLUDED.last_requester_id, last_played = NOW(), updated_at = NOW()]],
      guild_id, track_key(track.uri or url, track.title or title), track.uri or url, track.title or title, requester_id)

    if use_fade then
      local my_token = (fade_tokens[guild_id] or 0) + 1
      fade_tokens[guild_id] = my_token
      copas.addthread(function()
        local steps = 20
        local fade_seconds = settings.fade_seconds or 3.0
        for i = 1, steps do
          if fade_tokens[guild_id] ~= my_token then return end
          copas.sleep(fade_seconds / steps)
          if fade_tokens[guild_id] ~= my_token then return end
          local vol = math.floor((i / steps) * target_volume)
          bot.lavalink:set_volume(guild_id, vol)
        end
      end)
    end

    send_feedback_now_playing(guild_id, track.title or title, track.uri or url, track.author, track_uid, requester_id)
    log_info("[%s] now playing '%s'", guild_id, tostring(track.title))
  end)
  process_queue_busy[guild_id] = false
  if not ok then
    log_error("process_queue error for guild %s: %s", guild_id, tostring(err))
    report_error(guild_id, "runtime", "process_queue error", tostring(err))
  end
end

local function stop_playback(guild_id)
  bot.lavalink:stop(guild_id)
  clear_stage_topic(guild_id, playback[guild_id] and playback[guild_id].channel_id or get_home_channel_id(guild_id))
  playback[guild_id] = nil
  process_queue_busy[guild_id] = nil
end

-- ===========================================================================
-- Smart Auto-DJ (simplified): scores candidates from tunestream_track_intelligence
-- and tunestream_user_track_affinity instead of the Python bot's Gemini-embedding
-- similarity search. Still genuinely "learns" from likes/dislikes/finishes/
-- skips server-wide and per-user -- just without semantic embeddings.
-- ===========================================================================

local SMART_RADIO_SUFFIXES = { "radio", "audio", "playlist", "mix" }

local function clean_smart_title(title)
  if not title then return nil end
  local t = title:gsub("%[.-%]", ""):gsub("%(.-%)", ""):gsub("official%s*video", ""):gsub("official%s*audio", "")
  return trim(t)
end

local function record_track_feedback(guild_id, user_id, url, title, liked)
  local uk = track_key(url, title)
  local delta_like = liked and 1 or 0
  local delta_dislike = liked and 0 or 1
  q([[INSERT INTO tunestream_track_intelligence (guild_id, url_key, video_url, title, queued_count, play_count, finish_count, skip_count, like_count, dislike_count, total_listen_seconds, first_seen, updated_at)
      VALUES (%s, %s, %s, %s, 0, 0, 0, 0, %s, %s, 0, NOW(), NOW())
      ON CONFLICT (guild_id, url_key) DO UPDATE SET like_count = tunestream_track_intelligence.like_count + %s,
        dislike_count = tunestream_track_intelligence.dislike_count + %s, updated_at = NOW()]],
    guild_id, uk, url, title, delta_like, delta_dislike, delta_like, delta_dislike)
  local score_delta = liked and 4.0 or -4.0
  q([[INSERT INTO tunestream_user_track_affinity (guild_id, user_id, url_key, video_url, title, queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score, last_requested, updated_at)
      VALUES (%s, %s, %s, %s, %s, 0, 0, 0, 0, %s, %s, %s, NOW(), NOW())
      ON CONFLICT (guild_id, user_id, url_key) DO UPDATE SET
        like_count = tunestream_user_track_affinity.like_count + %s, dislike_count = tunestream_user_track_affinity.dislike_count + %s,
        score = tunestream_user_track_affinity.score + %s, updated_at = NOW()]],
    guild_id, user_id, uk, url, title, delta_like, delta_dislike, score_delta, delta_like, delta_dislike, score_delta)
end

local function get_current_track_snapshot(guild_id)
  local data = playback[guild_id]
  if not data then return nil end
  return { url = data.original_queue_url or data.url, title = data.original_queue_title or data.title, track_uid = data.track_uid }
end

-- pick_smart_recommendation(guild_id, listener_id) -> track{uri,title}, reason
local function pick_smart_recommendation(guild_id, listener_id)
  if listener_id then
    local row = q1([[SELECT video_url, title FROM tunestream_user_track_affinity
        WHERE guild_id = %s AND user_id = %s AND score > 0 ORDER BY score DESC, updated_at DESC LIMIT 1]], guild_id, listener_id)
    if row then return { uri = row.video_url, title = row.title }, "your saved taste" end
  end
  local row = q1([[SELECT video_url, title FROM tunestream_track_intelligence
      WHERE guild_id = %s ORDER BY (play_count + like_count * 2 - dislike_count * 2) DESC, last_played DESC LIMIT 1]], guild_id)
  if row then return { uri = row.video_url, title = row.title }, "server favorites" end
  return nil, nil
end

local function build_user_taste_summary(guild_id, user_id)
  local rows = q([[SELECT title, score FROM tunestream_user_track_affinity WHERE guild_id = %s AND user_id = %s ORDER BY score DESC LIMIT 8]], guild_id, user_id)
  local agg = q1([[SELECT COALESCE(SUM(play_count),0)::int AS played, COALESCE(SUM(finish_count),0)::int AS finished,
      COALESCE(SUM(like_count),0)::int AS liked, COALESCE(SUM(dislike_count),0)::int AS disliked,
      COALESCE(SUM(skip_count),0)::int AS skipped FROM tunestream_user_track_affinity WHERE guild_id = %s AND user_id = %s]], guild_id, user_id)
  return rows, agg
end

maybe_enqueue_autodj = function(guild_id, channel_id)
  if not get_autodj_enabled(guild_id) then return false end
  local chosen, reason = pick_smart_recommendation(guild_id, nil)
  local query
  if chosen and chosen.title then
    query = "ytmsearch:" .. clean_smart_title(chosen.title) .. " " .. SMART_RADIO_SUFFIXES[math.random(#SMART_RADIO_SUFFIXES)]
  else
    local fallback = { "lofi hip hop", "synthwave mix", "chill electronic", "gaming music", "jazz hop" }
    query = "ytmsearch:" .. fallback[math.random(#fallback)]
  end
  local entries = select(1, search_playables(query))
  if not entries or #entries == 0 then return false end
  local track = entries[1]
  enqueue_track(guild_id, track.uri, track.title, bot.user_id)
  copas.addthread(function() process_queue(guild_id, channel_id) end)
  return true
end

-- ===========================================================================
-- Lyrics (LRCLIB, no API key -- same endpoint the Python bot used)
-- ===========================================================================

local function parse_synced_lyrics(lrc)
  local lines = {}
  for line in lrc:gmatch("[^\r\n]+") do
    local mm, ss, cs, text = line:match("^%[(%d+):(%d+)%.?(%d*)%]%s*(.*)$")
    if mm then
      local ts = tonumber(mm) * 60 + tonumber(ss) + (tonumber(cs) and tonumber("0." .. cs) or 0)
      if trim(text) ~= "" then table.insert(lines, { ts = ts, text = trim(text) }) end
    end
  end
  table.sort(lines, function(a, b) return a.ts < b.ts end)
  return lines
end

local function fetch_lyrics(title, duration)
  local query = clean_smart_title(title)
  if not query or query == "" then return nil end
  local results = http_get_json("https://lrclib.net/api/search?q=" .. urlencode(query))
  if not results or #results == 0 then return nil end
  local best = results[1]
  if duration and duration > 0 then
    for _, r in ipairs(results) do
      if r.duration and math.abs(r.duration - duration) <= 5 then best = r; break end
    end
  end
  local synced = best.syncedLyrics
  local plain = best.plainLyrics
  if (not synced or synced == cjson.null) and (not plain or plain == cjson.null) then return nil end
  return {
    synced = (synced and synced ~= cjson.null) and parse_synced_lyrics(synced) or nil,
    plain = (plain ~= cjson.null) and plain or nil,
    track_name = best.trackName, artist_name = best.artistName,
  }
end

-- ===========================================================================
-- Lavalink event wiring: track end -> advance queue / loop / requeue.
-- ===========================================================================

bot.lavalink:on("event", function(msg)
  local guild_id = msg.guildId
  if not guild_id then return end
  if msg.type == "TrackStartEvent" then
    log_info("[%s] TrackStartEvent %s", guild_id, msg.track and msg.track.info and msg.track.info.title or "?")
  elseif msg.type == "TrackEndEvent" then
    if msg.reason == "replaced" then return end
    local data = playback[guild_id]
    local settings = get_settings(guild_id)
    local channel_id = data and data.channel_id
    local url = data and (data.original_queue_url or data.url)
    local title = data and (data.original_queue_title or data.title)
    local requester_id = data and data.requester_id
    local track_uid = data and data.track_uid

    if msg.reason == "finished" then
      if settings.loop_mode == "queue" and url then
        enqueue_track(guild_id, url, title, requester_id, track_uid)
        snapshot_backup(guild_id)
      elseif settings.loop_mode == "song" and url then
        insert_queue_front(guild_id, url, title, requester_id, track_uid)
      else
        delete_backup_track(guild_id, track_uid, url, title)
      end
      if url then
        local uk = track_key(url, title)
        q("UPDATE tunestream_track_intelligence SET finish_count = finish_count + 1, total_listen_seconds = total_listen_seconds + %s, updated_at = NOW() WHERE guild_id = %s AND url_key = %s",
          math.floor(data and data.duration or 0), guild_id, uk)
      end
    elseif msg.reason == "loadFailed" or msg.reason == "cleanup" then
      log_warn("[%s] track load failed/cleanup: %s", guild_id, title or "?")
    else
      -- stopped / skipped manually
      if url then
        local uk = track_key(url, title)
        q("UPDATE tunestream_track_intelligence SET skip_count = skip_count + 1, updated_at = NOW() WHERE guild_id = %s AND url_key = %s", guild_id, uk)
      end
    end

    if channel_id then
      copas.addthread(function() process_queue(guild_id, channel_id) end)
    end
  elseif msg.type == "TrackStuckEvent" or msg.type == "TrackExceptionEvent" then
    log_warn("[%s] %s: %s", guild_id, msg.type, cjson.encode(msg))
    local data = playback[guild_id]
    if data and data.channel_id then
      copas.addthread(function() process_queue(guild_id, data.channel_id) end)
    end
  end
end)

-- ===========================================================================
-- Slash command registration + handlers
-- ===========================================================================

local OPT = { STRING = 3, INTEGER = 4, BOOLEAN = 5, USER = 6, CHANNEL = 7, ROLE = 8, NUMBER = 10 }
local CH = { GUILD_TEXT = 0, GUILD_VOICE = 2, GUILD_ANNOUNCEMENT = 5, GUILD_STAGE_VOICE = 13, ANNOUNCEMENT_THREAD = 10, PUBLIC_THREAD = 11, PRIVATE_THREAD = 12 }
local ADMIN_ONLY = tostring(ADMINISTRATOR)

local function cmdname(n) return "tunestream_main_" .. n end

-- ---- Guild configuration (administrator-only via default_member_permissions) ----

bot:command(cmdname("sethome"), {
  description = "Save this bot's default voice or stage channel for join, autoplay, and recovery behavior.",
  default_member_permissions = ADMIN_ONLY,
  options = {
    { type = OPT.CHANNEL, name = "channel", description = "Voice or stage channel", required = true, channel_types = { CH.GUILD_VOICE, CH.GUILD_STAGE_VOICE } },
  },
}, function(_, interaction)
  local channel_id = get_opt(interaction, "channel")
  q([[INSERT INTO tunestream_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, 'tunestream', %s)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET home_vc_id = EXCLUDED.home_vc_id]], guild_id_of(interaction), channel_id)
  ack_embed(interaction, embed("Home channel set to <#" .. channel_id .. ">.", { title = "\xF0\x9F\x8F\xA0 Home Set", color = COLOR.green }), true)
end)

bot:command(cmdname("setfeedback"), {
  description = "Choose the text channel (or thread) for updates, queue actions, and recovery notices.",
  default_member_permissions = ADMIN_ONLY,
  options = {
    { type = OPT.CHANNEL, name = "channel", description = "Text channel or thread", required = true,
      channel_types = { CH.GUILD_TEXT, CH.GUILD_ANNOUNCEMENT, CH.ANNOUNCEMENT_THREAD, CH.PUBLIC_THREAD, CH.PRIVATE_THREAD } },
  },
}, function(_, interaction)
  local channel_id = get_opt(interaction, "channel")
  ensure_guild_settings(guild_id_of(interaction))
  q("UPDATE tunestream_guild_settings SET feedback_channel_id = %s WHERE guild_id = %s", channel_id, guild_id_of(interaction))
  ack_embed(interaction, embed("Updates will be sent to <#" .. channel_id .. ">.", { title = "\xE2\x9C\x85 Feedback Channel Set", color = COLOR.green }), true)
end)

bot:command(cmdname("djrole"), {
  description = "Set the server DJ role that can manage restricted playback, queue, and settings commands.",
  default_member_permissions = ADMIN_ONLY,
  options = { { type = OPT.ROLE, name = "role", description = "DJ role", required = true } },
}, function(_, interaction)
  local role_id = get_opt(interaction, "role")
  ensure_guild_settings(guild_id_of(interaction))
  q("UPDATE tunestream_guild_settings SET dj_role_id = %s WHERE guild_id = %s", role_id, guild_id_of(interaction))
  ack_embed(interaction, embed("\xF0\x9F\x8E\xA7 DJ role set to <@&" .. role_id .. ">", { color = COLOR.green }), true)
end)

bot:command(cmdname("removedj"), {
  description = "Clear the configured DJ role so only admins or open-access mode can control restricted commands.",
  default_member_permissions = ADMIN_ONLY,
}, function(_, interaction)
  ensure_guild_settings(guild_id_of(interaction))
  q("UPDATE tunestream_guild_settings SET dj_role_id = NULL WHERE guild_id = %s", guild_id_of(interaction))
  ack_embed(interaction, embed("DJ role requirements removed.", { color = COLOR.green }), true)
end)

bot:command(cmdname("djmode"), {
  description = "Enable or disable Strict DJ Mode so only admins and the DJ role can use control commands.",
  default_member_permissions = ADMIN_ONLY,
}, function(_, interaction)
  local gid = guild_id_of(interaction)
  ensure_guild_settings(gid)
  local row = q1("SELECT dj_only_mode FROM tunestream_guild_settings WHERE guild_id = %s", gid)
  local new_val = not (row and row.dj_only_mode)
  q("UPDATE tunestream_guild_settings SET dj_only_mode = %s WHERE guild_id = %s", new_val, gid)
  ack_embed(interaction, embed("\xF0\x9F\x8E\xA7 Strict DJ Mode is now **" .. (new_val and "ENABLED" or "DISABLED") .. "**.", { color = COLOR.green }), true)
end)

bot:command(cmdname("247"), {
  description = "Keep the bot connected and ready in voice channels even after playback ends until you disable it.",
  default_member_permissions = ADMIN_ONLY,
}, function(_, interaction)
  local gid = guild_id_of(interaction)
  ensure_guild_settings(gid)
  local row = q1("SELECT stay_in_vc FROM tunestream_guild_settings WHERE guild_id = %s", gid)
  local new_val = not (row and row.stay_in_vc)
  q("UPDATE tunestream_guild_settings SET stay_in_vc = %s WHERE guild_id = %s", new_val, gid)
  ack_embed(interaction, embed("\xF0\x9F\x95\xB0\xEF\xB8\x8F 24/7 Mode is now **" .. (new_val and "ENABLED" or "DISABLED") .. "**.", { color = COLOR.green }), true)
end)

bot:command(cmdname("restart"), {
  description = "Restart this bot instance immediately for maintenance or recovery. Administrator only.",
  default_member_permissions = ADMIN_ONLY,
}, function(_, interaction)
  ack(interaction, "Restarting...", true)
  copas.addthread(function() copas.sleep(1); os.exit(0) end) -- supervisor (docker/compose restart policy) brings it back
end)

-- ---- Search autocomplete ----

local function youtube_suggest(query)
  local data = http_get_json("https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=" .. urlencode(query))
  if not data or not data[2] then return {} end
  local out = {}
  for _, s in ipairs(data[2]) do
    if #out >= 10 then break end
    table.insert(out, tostring(s))
  end
  return out
end

local function queue_index_choices(guild_id, needle)
  local rows = q("SELECT title FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT 200", guild_id)
  needle = trim(needle or ""):lower()
  local out = {}
  for i, r in ipairs(rows) do
    local title = r.title or "Unknown title"
    if needle == "" or needle == tostring(i) or title:lower():find(needle, 1, true) then
      local label = string.format("%d. %s", i, title):sub(1, 95)
      table.insert(out, { name = label, value = i })
      if #out >= 25 then break end
    end
  end
  return out
end

-- ---- Playback & transport ----

bot:command(cmdname("play"), {
  description = "Queue a track, URL, livestream, search result, or playlist and start playback if idle.",
  options = {
    { type = OPT.STRING, name = "search", description = "Track name, URL, or search text", required = true, autocomplete = true },
    { type = OPT.STRING, name = "source", description = "Where to search (defaults to YouTube for plain text)", required = false,
      choices = { { name = "YouTube", value = "ytmsearch" }, { name = "SoundCloud", value = "scsearch" } } },
  },
}, function(_, interaction)
  local search = get_opt(interaction, "search")
  local source = get_opt(interaction, "source") or "ytmsearch"
  if (source == "ytmsearch" or source == "scsearch") and not is_explicit_lavalink_query(search) then
    search = source .. ":" .. search
  end
  defer(interaction, false)
  local gid = guild_id_of(interaction)

  local channel_id = get_home_channel_id(gid)
  if not channel_id then
    channel_id = interaction.member and interaction.member.voice_channel_id -- not populated by gateway payload; fall back below
  end
  if not channel_id then
    -- Discord doesn't include the invoking member's voice state on the interaction payload;
    -- rely on the home channel or a prior /join. If neither, ask the user to join a channel.
    local row = q1("SELECT connected_channel_id FROM tunestream_voice_state WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
    channel_id = row and row.connected_channel_id
  end
  if not channel_id then
    followup_embed(interaction, embed("Join a channel first or set a home channel with /tunestream_main_sethome.", { title = "\xE2\x9D\x8C Error", color = COLOR.red }))
    return
  end

  local entries, playlist_name, err = search_playables(search)
  if err or #entries == 0 then
    followup_embed(interaction, embed("Could not load that source: " .. (err or "nothing playable came back."), { title = "\xE2\x9D\x8C Source Error", color = COLOR.red }))
    return
  end

  local user_id = interaction.member and interaction.member.user and interaction.member.user.id
  for _, t in ipairs(entries) do enqueue_track(gid, t.uri, t.title, user_id) end
  if #entries > 1 then shuffle_queue(gid, true) end
  snapshot_backup(gid)
  if playlist_name and is_playlist_source(search) then
    set_active_playlist(gid, search, entries, user_id, channel_id)
  end
  local qlen = queue_count(gid)

  local playing = playback[gid] ~= nil
  if not playing then
    followup_embed(interaction, embed(string.format("Added **%d** tracks. Starting Lavalink Engine!", #entries), { title = "\xF0\x9F\x8E\xB6 Queued & Starting", color = COLOR.green }))
    copas.addthread(function() process_queue(gid, channel_id) end)
  else
    followup_embed(interaction, embed(string.format("Added **%d** tracks. (Queue size: %d)", #entries, qlen), { title = "\xF0\x9F\x93\xA5 Added to Queue", color = COLOR.blue }))
  end
end)

bot:command(cmdname("playnext"), {
  description = "Queue one track to play next, ahead of the existing queue, without clearing current playback.",
  options = { { type = OPT.STRING, name = "search", description = "Track name or URL", required = true } },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  local search = get_opt(interaction, "search")
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local entries, _pn, err = search_playables(search)
  if err or #entries == 0 then
    followup_embed(interaction, embed("Could not resolve that source: " .. (err or "not found"), { color = COLOR.red }))
    return
  end
  local track = entries[1]
  local user_id = interaction.member and interaction.member.user and interaction.member.user.id
  insert_queue_front(gid, track.uri, track.title, user_id)
  snapshot_backup(gid)
  if not playback[gid] then
    local channel_id = get_home_channel_id(gid)
    if channel_id then copas.addthread(function() process_queue(gid, channel_id) end) end
  end
  followup_embed(interaction, embed("**Playing next:** " .. track.title, { color = COLOR.green }))
end)

bot:command(cmdname("skip"), { description = "Skip the current track and move playback to the next queued item or Auto-DJ recommendation." },
function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  if playback[gid] then
    bot.lavalink:stop(gid)
    ack_embed(interaction, embed("\xE2\x8F\xAD\xEF\xB8\x8F Skipped", { color = COLOR.blurple }), true)
  else
    ack_embed(interaction, embed("\xE2\x9D\x8C Nothing is playing.", { color = COLOR.red }), true)
  end
end)

bot:command(cmdname("stop"), { description = "Stop playback, clear the queue, remove recovery state, and reset the bot for this server." },
function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  stop_playback(gid)
  clear_active_playlist(gid)
  ack_embed(interaction, embed("Music stopped and cleared.", { title = "\xE2\x8F\xB9\xEF\xB8\x8F Stopped", color = COLOR.red }), true)
end)

bot:command(cmdname("sleep"), {
  description = "Stop playback and disconnect after N minutes. Use 0 to cancel an active timer.",
  options = { { type = OPT.INTEGER, name = "minutes", description = "Minutes until playback stops automatically (0 cancels the current timer)", required = true, min_value = 0, max_value = 240 } },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local minutes = get_opt(interaction, "minutes")
  local existing = sleep_timers[gid]
  if existing then existing.cancel = true end
  if minutes <= 0 then
    ack_embed(interaction, embed("\xE2\x8F\xB0 Sleep timer cancelled.", { color = COLOR.orange }), true)
    return
  end
  local token = { cancel = false }
  sleep_timers[gid] = token
  copas.addthread(function()
    copas.sleep(minutes * 60)
    if token.cancel then return end
    sleep_timers[gid] = nil
    stop_playback(gid)
    clear_active_playlist(gid)
    log_info("[%s] sleep timer elapsed, playback stopped", gid)
  end)
  ack_embed(interaction, embed(string.format("\xF0\x9F\x98\xB4 Sleep timer set for **%d** minute(s). Playback will stop automatically.", minutes), { color = COLOR.blue }), true)
end)

bot:command(cmdname("pause"), { description = "Pause the current track without clearing the queue or playback position." },
function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local data = playback[gid]
  if data and not data.paused then
    bot.lavalink:set_paused(gid, true)
    data.offset = current_position(gid); data.paused = true
    ack_embed(interaction, embed("\xE2\x8F\xB8\xEF\xB8\x8F Paused", { color = COLOR.blue }), true)
  else
    ack_embed(interaction, embed("\xE2\x9D\x8C Nothing is currently playing.", { color = COLOR.red }), true)
  end
end)

bot:command(cmdname("resume"), { description = "Resume the paused track from its current playback position." },
function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local data = playback[gid]
  if data and data.paused then
    bot.lavalink:set_paused(gid, false)
    data.start_time = socket.gettime(); data.paused = false
    ack_embed(interaction, embed("\xE2\x96\xB6\xEF\xB8\x8F Resumed", { color = COLOR.green }), true)
  else
    ack_embed(interaction, embed("\xE2\x9D\x8C Nothing is currently paused.", { color = COLOR.red }), true)
  end
end)

-- Factored out (rather than left inline like rhythm.lua) so db_smoke_test.lua
-- can exercise the exact INSERT/UPDATE shape that hits the NOT-NULL
-- reconnect_attempts column (tunestream_voice_state) and the playback_state
-- reset columns directly, without needing a live Discord interaction.
local function clear_playback_state(guild_id)
  q([[UPDATE tunestream_playback_state SET channel_id = NULL, video_url = NULL, title = NULL, position_seconds = 0,
      is_playing = false, is_paused = false, play_session_key = NULL, track_uid = NULL WHERE guild_id = %s AND bot_name = 'tunestream']], guild_id)
end

local function mark_voice_connected(guild_id, channel_id)
  q([[INSERT INTO tunestream_voice_state (guild_id, bot_name, connected_channel_id, desired_connected, last_seen_at, reconnect_attempts)
      VALUES (%s, 'tunestream', %s, true, NOW(), 0)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET connected_channel_id = EXCLUDED.connected_channel_id, desired_connected = true, last_seen_at = NOW()]],
    guild_id, channel_id)
end

local function mark_voice_disconnected(guild_id)
  q([[UPDATE tunestream_voice_state SET desired_connected = false, connected_channel_id = NULL, disconnected_at = NOW() WHERE guild_id = %s AND bot_name = 'tunestream']], guild_id)
end

bot:command(cmdname("clear"), { description = "Clear the upcoming queue, stop playback, and reset stored playback state for this server." },
function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  stop_playback(gid)
  clear_active_playlist(gid)
  q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
  q("DELETE FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
  clear_playback_state(gid)
  ack_embed(interaction, embed("\xF0\x9F\x97\x91\xEF\xB8\x8F Playback stopped and queue cleared.", { color = COLOR.red }), true)
end)

bot:command(cmdname("join"), { description = "Force the bot to join your current voice channel, or its configured home channel if one is saved." },
function(_, interaction)
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local channel_id = get_home_channel_id(gid)
  if not channel_id then
    followup_content(interaction, "Join a channel first, or set a home channel.", true)
    return
  end
  ensure_voice_connection(gid, channel_id)
  mark_voice_connected(gid, channel_id)
  followup_embed(interaction, embed("Joined <#" .. channel_id .. ">.", { color = COLOR.green }))
end)

bot:command(cmdname("leave"), { description = "Disconnect the bot from voice and clear any pending recovery handoff for this server." },
function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  stop_playback(gid)
  clear_active_playlist(gid)
  q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
  q("DELETE FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
  bot.gateway:leave_voice(gid)
  mark_voice_disconnected(gid)
  ack_embed(interaction, embed("Left the channel.", { color = COLOR.orange }), true)
end)

-- ---- Queue management ----

bot:command(cmdname("queue"), {
  description = "Show the current queue with paging, requester names, and track positions",
  options = { { type = OPT.INTEGER, name = "page", description = "Page number", required = false, min_value = 1 } },
}, function(_, interaction)
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local page = math.max(1, get_opt(interaction, "page") or 1)
  local per_page = 10
  local offset = (page - 1) * per_page
  local total = queue_count(gid)
  local rows = q("SELECT title, requester_id FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT %s OFFSET %s", gid, per_page, offset)
  if #rows == 0 then
    followup_embed(interaction, embed("Queue empty.", { color = COLOR.red }))
    return
  end
  local lines = {}
  for i, r in ipairs(rows) do
    lines[#lines + 1] = string.format("**%d.** %s — *%s*", offset + i, r.title, resolve_requester_name(gid, r.requester_id))
  end
  local pages = math.max(1, math.ceil(total / per_page))
  followup_embed(interaction, embed(table.concat(lines, "\n"), { title = "\xF0\x9F\x93\x9C Queue", color = COLOR.blurple, footer = string.format("Page %d/%d \xE2\x80\xA2 %d queued track(s)", page, pages, total) }))
end)

bot:command(cmdname("shuffle"), { description = "Smart-shuffle the upcoming queue while keeping duplicate tracks separated when possible." },
function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local n = shuffle_queue(gid, false)
  if n <= 0 then ack(interaction, "Queue empty.", true); return end
  snapshot_backup(gid)
  ack_embed(interaction, embed(string.format("\xF0\x9F\x94\x80 Smart-shuffled %d queued track(s), keeping repeat tracks apart where possible.", n), { color = COLOR.green }), true)
end)

local function index_command(name, description, dj_required, handler)
  bot:command(cmdname(name), {
    description = description,
    options = { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, autocomplete = true } },
  }, function(_, interaction)
    if dj_required and not is_dj(interaction) then return end
    handler(interaction, get_opt(interaction, "index"))
  end)
end

index_command("remove", "Remove a queued track by its queue number so it will not play later.", true, function(interaction, index)
  local gid = guild_id_of(interaction)
  if index < 1 then ack(interaction, "Invalid index.", true); return end
  local row = q1("SELECT id, video_url, title, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT 1 OFFSET %s", gid, index - 1)
  if not row then ack(interaction, "Invalid index.", true); return end
  q("DELETE FROM tunestream_queue WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", row.id, gid)
  delete_backup_track(gid, col(row, "track_uid"), row.video_url, row.title)
  ack_embed(interaction, embed("Removed item #" .. index, { color = COLOR.green }), true)
end)

index_command("skipto", "Drop everything before a chosen queue position and jump playback forward to that track.", true, function(interaction, index)
  local gid = guild_id_of(interaction)
  if index < 1 then ack(interaction, "Invalid index.", true); return end
  defer(interaction, true)
  local skip_n = index - 1
  local rows = q("SELECT video_url, title, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT %s", gid, skip_n)
  if #rows > 0 then
    q("DELETE FROM tunestream_queue WHERE id IN (SELECT id FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT %s)", gid, skip_n)
    for _, r in ipairs(rows) do delete_backup_track(gid, col(r, "track_uid"), r.video_url, r.title) end
  end
  if playback[gid] then bot.lavalink:stop(gid) end
  followup_embed(interaction, embed("Skipped to #" .. index, { color = COLOR.green }))
end)

bot:command(cmdname("move"), {
  description = "Move a queued track from one queue slot to another without rebuilding the entire session manually.",
  options = {
    { type = OPT.INTEGER, name = "frm", description = "From position", required = true, autocomplete = true },
    { type = OPT.INTEGER, name = "to", description = "To position", required = true, autocomplete = true },
  },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local frm, to = get_opt(interaction, "frm"), get_opt(interaction, "to")
  local rows = q("SELECT id, video_url, title, requester_id, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", gid)
  if frm > #rows or to > #rows or frm < 1 or to < 1 then
    followup_content(interaction, "Invalid index", true); return
  end
  local item = table.remove(rows, frm)
  table.insert(rows, clamp(to, 1, #rows + 1), item)
  q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
  for _, r in ipairs(rows) do
    q("INSERT INTO tunestream_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'tunestream', %s, %s, %s, %s)",
      gid, r.video_url, r.title, r.requester_id, col(r, "track_uid") or new_uid())
  end
  snapshot_backup(gid)
  followup_embed(interaction, embed(string.format("Moved item from %d to %d", frm, to), { color = COLOR.green }))
end)

index_command("bump", "Move a queued track to the front so it plays next", true, function(interaction, index)
  local gid = guild_id_of(interaction)
  if index < 1 then ack(interaction, "Invalid index.", true); return end
  local row = q1("SELECT id, video_url, title, requester_id, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT 1 OFFSET %s", gid, index - 1)
  if not row then ack(interaction, "Invalid index.", true); return end
  q("DELETE FROM tunestream_queue WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", row.id, gid)
  insert_queue_front(gid, row.video_url, row.title, row.requester_id, col(row, "track_uid"))
  snapshot_backup(gid)
  ack_embed(interaction, embed("\xE2\xAC\x86\xEF\xB8\x8F Moved **" .. row.title .. "** to play next.", { color = COLOR.green }), true)
end)

bot:command(cmdname("clearmine"), { description = "Remove your own queued songs without touching other listeners' tracks" },
function(_, interaction)
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  local row = q1("SELECT COUNT(*)::int AS c FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' AND requester_id = %s", gid, uid)
  local removed = row and row.c or 0
  if removed > 0 then
    q("DELETE FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' AND requester_id = %s", gid, uid)
    q("DELETE FROM tunestream_queue_backup WHERE guild_id = %s AND bot_name = 'tunestream' AND requester_id = %s", gid, uid)
  end
  followup_embed(interaction, embed(string.format("\xF0\x9F\xA7\xB9 Removed **%d** of your queued track(s).", removed), { color = COLOR.green }))
end)

bot:command(cmdname("voteskip"), { description = "Start or join a vote skip when no DJ is around to skip directly" },
function(_, interaction)
  local gid = guild_id_of(interaction)
  if not playback[gid] then ack(interaction, "Nothing is playing right now.", true); return end
  if is_dj(interaction, true) then
    bot.lavalink:stop(gid)
    ack_embed(interaction, embed("\xE2\x8F\xAD\xEF\xB8\x8F DJ override used: skipped the current track.", { color = COLOR.green }), true)
    return
  end
  vote_skip_sessions[gid] = vote_skip_sessions[gid] or {}
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  vote_skip_sessions[gid][uid] = true
  local count = 0
  for _ in pairs(vote_skip_sessions[gid]) do count = count + 1 end
  local required = 2 -- without a live voice-channel member list we can't compute exact half; use a small fixed quorum
  if count >= required then
    vote_skip_sessions[gid] = nil
    bot.lavalink:stop(gid)
    ack_embed(interaction, embed(string.format("\xE2\x8F\xAD\xEF\xB8\x8F Vote skip passed with **%d/%d** votes.", count, required), { color = COLOR.green }), false)
  else
    ack_embed(interaction, embed(string.format("\xF0\x9F\x97\xB3\xEF\xB8\x8F Vote recorded: **%d/%d** votes to skip.", count, required), { color = COLOR.blurple }), true)
  end
end)

local function vote_command(name, description, is_up)
  bot:command(cmdname(name), {
    description = description,
    options = { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true } },
  }, function(_, interaction)
    local gid = guild_id_of(interaction)
    local index = get_opt(interaction, "index")
    if not playback[gid] then ack(interaction, "Nothing is playing right now.", true); return end
    if index < 1 then ack(interaction, "Invalid index.", true); return end
    local row = q1("SELECT id, video_url, title, requester_id, track_uid FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT 1 OFFSET %s", gid, index - 1)
    if not row then ack(interaction, "Invalid index.", true); return end
    local key = gid .. ":" .. row.id
    local uid = interaction.member and interaction.member.user and interaction.member.user.id
    local up = queue_upvotes[key] or {}; queue_upvotes[key] = up
    local down = queue_downvotes[key] or {}; queue_downvotes[key] = down
    local required = 2
    if is_up then
      down[uid] = nil; up[uid] = true
      local n = 0; for _ in pairs(up) do n = n + 1 end
      if n >= required then
        queue_upvotes[key] = nil; queue_downvotes[key] = nil
        q("DELETE FROM tunestream_queue WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", row.id, gid)
        insert_queue_front(gid, row.video_url, row.title, row.requester_id, col(row, "track_uid"))
        snapshot_backup(gid)
        ack_embed(interaction, embed("\xE2\xAC\x86\xEF\xB8\x8F **" .. row.title .. "** got enough upvotes and will play next!", { color = COLOR.green }), false)
      else
        ack_embed(interaction, embed(string.format("\xF0\x9F\x94\xBA Upvoted **#%d** (%d/%d) to play next.", index, n, required), { color = COLOR.blurple }), true)
      end
    else
      up[uid] = nil; down[uid] = true
      local n = 0; for _ in pairs(down) do n = n + 1 end
      if n >= required then
        queue_upvotes[key] = nil; queue_downvotes[key] = nil
        q("DELETE FROM tunestream_queue WHERE id = %s AND guild_id = %s AND bot_name = 'tunestream'", row.id, gid)
        delete_backup_track(gid, col(row, "track_uid"), row.video_url, row.title)
        ack_embed(interaction, embed("\xE2\xAC\x87\xEF\xB8\x8F **" .. row.title .. "** got enough downvotes and was removed from the queue.", { color = COLOR.orange }), false)
      else
        ack_embed(interaction, embed(string.format("\xF0\x9F\x94\xBB Downvoted **#%d** (%d/%d).", index, n, required), { color = COLOR.blurple }), true)
      end
    end
  end)
end
vote_command("upvote", "Upvote a queued track; enough listener upvotes bump it to play next.", true)
vote_command("downvote", "Downvote a queued track; enough listener downvotes removes it from the queue.", false)

-- ---- Radio / Auto-DJ / discovery ----

bot:command(cmdname("radio"), {
  description = "Queue several tracks matching a mood or vibe, e.g. 'chill lofi' or 'workout hype'.",
  options = {
    { type = OPT.STRING, name = "mood", description = "Mood or vibe", required = true },
    { type = OPT.INTEGER, name = "count", description = "How many tracks (1-10)", required = false, min_value = 1, max_value = 10 },
  },
}, function(_, interaction)
  defer(interaction, false)
  local gid = guild_id_of(interaction)
  local mood = get_opt(interaction, "mood")
  local count = get_opt(interaction, "count") or 5
  local channel_id = get_home_channel_id(gid)
  if not channel_id then
    followup_embed(interaction, embed("Join a voice channel first.", { color = COLOR.red })); return
  end
  local entries, _pn, err = search_playables(mood:sub(1, 100) .. " mix")
  if err or #entries == 0 then
    followup_embed(interaction, embed("Couldn't find tracks for **" .. mood .. "**.", { color = COLOR.red })); return
  end
  local n = math.min(count, #entries)
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  for i = 1, n do enqueue_track(gid, entries[i].uri, entries[i].title, uid) end
  snapshot_backup(gid)
  if not playback[gid] then copas.addthread(function() process_queue(gid, channel_id) end) end
  followup_embed(interaction, embed(string.format("Queued **%d** track(s) for the vibe: *%s*.", n, mood), { title = "\xF0\x9F\x93\xBB Mood Radio", color = COLOR.green }))
end)

bot:command(cmdname("autodj"), {
  description = "Enable or disable smarter Auto-DJ recommendations when the queue runs dry",
  options = { { type = OPT.BOOLEAN, name = "enabled", description = "Enable Auto-DJ", required = true } },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  defer(interaction, true)
  local enabled = get_opt(interaction, "enabled")
  set_autodj_enabled(guild_id_of(interaction), enabled)
  followup_embed(interaction, embed("\xF0\x9F\x93\xBB Auto-DJ is now **" .. (enabled and "enabled" or "disabled") .. "**.", { color = COLOR.green }))
end)

bot:command(cmdname("settings"), { description = "Show the saved playback, DJ, queue, and recovery settings for this server" },
function(_, interaction)
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local s = get_settings(gid)
  local autodj = get_autodj_enabled(gid)
  local e = embed(nil, { title = "\xE2\x9A\x99\xEF\xB8\x8F Server Music Settings", color = COLOR.blurple })
  e.fields = {
    { name = "Home Channel", value = s.home_vc_id and ("<#" .. s.home_vc_id .. ">") or "Not set", inline = true },
    { name = "Feedback Channel", value = s.feedback_channel_id and ("<#" .. s.feedback_channel_id .. ">") or "Not set", inline = true },
    { name = "DJ Role", value = s.dj_role_id and ("<@&" .. s.dj_role_id .. ">") or "Not set", inline = true },
    { name = "Volume", value = tostring(s.volume), inline = true },
    { name = "Loop Mode", value = tostring(s.loop_mode), inline = true },
    { name = "Filter", value = tostring(s.filter_mode), inline = true },
    { name = "Transitions", value = tostring(s.transition_mode), inline = true },
    { name = "Custom Speed/Pitch", value = string.format("%sx / %sx (%s left)", s.custom_speed, s.custom_pitch, s.custom_modifiers_left), inline = true },
    { name = "Strict DJ", value = s.dj_only_mode and "Enabled" or "Disabled", inline = true },
    { name = "24/7 Mode", value = s.stay_in_vc and "Enabled" or "Disabled", inline = true },
    { name = "Auto-DJ", value = autodj and "Enabled" or "Disabled", inline = true },
  }
  followup_embed(interaction, e)
end)

-- ---- Playlists ----

bot:command(cmdname("playlists"), { description = "List your saved personal playlists and how many tracks each one contains" },
function(_, interaction)
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  local rows = q("SELECT playlist_name, COUNT(*)::int AS c FROM tunestream_user_playlists WHERE user_id = %s GROUP BY playlist_name ORDER BY playlist_name ASC", uid)
  if #rows == 0 then ack(interaction, "You do not have any saved playlists yet.", true); return end
  local lines = {}
  for i = 1, math.min(20, #rows) do lines[#lines + 1] = string.format("\xE2\x80\xA2 **%s** — %d track(s)", rows[i].playlist_name, rows[i].c) end
  ack_embed(interaction, embed(table.concat(lines, "\n"), { title = "\xF0\x9F\x8E\xBC Your Saved Playlists", color = COLOR.blurple }), true)
end)

bot:command(cmdname("deleteplaylist"), {
  description = "Delete one of your saved personal playlists by name",
  options = { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
}, function(_, interaction)
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  local name = get_opt(interaction, "name")
  local row = q1("SELECT COUNT(*)::int AS c FROM tunestream_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name)
  local count = row and row.c or 0
  if count == 0 then ack(interaction, "That playlist was not found.", true); return end
  q("DELETE FROM tunestream_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name)
  ack_embed(interaction, embed(string.format("\xF0\x9F\x97\x91\xEF\xB8\x8F Deleted **%s** (%d track(s)).", name, count), { color = COLOR.green }), true)
end)

bot:command(cmdname("savequeue"), {
  description = "Save the current queue to one of your personal playlists so you can load it again later.",
  options = { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
}, function(_, interaction)
  defer(interaction, true)
  local gid, uid = guild_id_of(interaction), interaction.member and interaction.member.user and interaction.member.user.id
  local name = get_opt(interaction, "name")
  local rows = q("SELECT video_url, title FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC", gid)
  if #rows == 0 then followup_content(interaction, "Queue is empty!", true); return end
  for _, r in ipairs(rows) do
    q("INSERT INTO tunestream_user_playlists (user_id, playlist_name, video_url, title) VALUES (%s, %s, %s, %s)", uid, name, r.video_url, r.title)
  end
  followup_embed(interaction, embed(string.format("\xF0\x9F\x92\xBE Saved **%d** tracks to your personal playlist: **%s**", #rows, name), { color = COLOR.green }))
end)

bot:command(cmdname("loadqueue"), {
  description = "Load one of your saved personal playlists into the active queue and start playback if needed.",
  options = { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
}, function(_, interaction)
  defer(interaction, true)
  local gid, uid = guild_id_of(interaction), interaction.member and interaction.member.user and interaction.member.user.id
  local name = get_opt(interaction, "name")
  local rows = q("SELECT video_url, title FROM tunestream_user_playlists WHERE user_id = %s AND playlist_name = %s", uid, name)
  if #rows == 0 then followup_content(interaction, "Playlist not found or empty.", true); return end
  for _, r in ipairs(rows) do enqueue_track(gid, r.video_url, r.title, uid) end
  snapshot_backup(gid)
  followup_embed(interaction, embed(string.format("\xF0\x9F\x93\x82 Loaded **%d** tracks from **%s** into the queue!", #rows, name), { color = COLOR.green }))
  if not playback[gid] then
    local channel_id = get_home_channel_id(gid)
    if channel_id then copas.addthread(function() process_queue(gid, channel_id) end) end
  end
end)

-- ---- History / leaderboard / social ----

bot:command(cmdname("leaderboard"), { description = "Show the most played tracks from this server based on stored playback history." },
function(_, interaction)
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local rows = q("SELECT title, COUNT(*)::int AS plays FROM tunestream_history WHERE guild_id = %s GROUP BY title ORDER BY plays DESC LIMIT 10", gid)
  if #rows == 0 then followup_content(interaction, "No play history yet.", true); return end
  local lines = {}
  for i, r in ipairs(rows) do lines[#lines + 1] = string.format("**%d.** %s *(Played %d times)*", i, r.title, r.plays) end
  followup_embed(interaction, embed(table.concat(lines, "\n"), { title = "\xF0\x9F\x8F\x86 Server Top Tracks", color = COLOR.gold }))
end)

bot:command(cmdname("history"), { description = "Show the most recent tracks played in this server from playback history." },
function(_, interaction)
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local rows = q("SELECT title FROM tunestream_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 5", gid)
  if #rows == 0 then followup_content(interaction, "No history.", true); return end
  local lines = {}
  for _, r in ipairs(rows) do lines[#lines + 1] = "- " .. r.title end
  followup_embed(interaction, embed(table.concat(lines, "\n"), { title = "\xF0\x9F\x93\x9C History", color = COLOR.blurple }))
end)

bot:command(cmdname("userhistory"), {
  description = "Show the most recent tracks requested by a specific user in this server.",
  options = { { type = OPT.USER, name = "member", description = "Member", required = true } },
}, function(_, interaction)
  local gid = guild_id_of(interaction)
  local member_id = get_opt(interaction, "member")
  local rows = q("SELECT id, title, video_url FROM tunestream_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 10", gid, member_id)
  local name = resolve_requester_name(gid, member_id)
  if #rows == 0 then
    ack_embed(interaction, embed(string.format("\xF0\x9F\x93\xAD %s hasn't queued any songs yet.", name), { color = COLOR.red }), true)
    return
  end
  local lines = {}
  for i, r in ipairs(rows) do lines[#lines + 1] = string.format("**%d.** [%s](%s)", i, r.title, r.video_url) end
  ack_embed(interaction, embed(table.concat(lines, "\n"), { title = "\xF0\x9F\x8E\xA7 " .. name .. "'s Play History", color = COLOR.blue, footer = "Use /tunestream_main_steal <user> <number> to add one to the queue!" }), true)
end)

bot:command(cmdname("steal"), {
  description = "Copy a track from a member's request history and add it back into the queue.",
  options = {
    { type = OPT.USER, name = "member", description = "Member", required = true },
    { type = OPT.INTEGER, name = "track_number", description = "Track number from their history", required = true, min_value = 1 },
  },
}, function(_, interaction)
  local gid = guild_id_of(interaction)
  local member_id = get_opt(interaction, "member")
  local track_number = get_opt(interaction, "track_number")
  local row = q1("SELECT video_url, title FROM tunestream_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 1 OFFSET %s", gid, member_id, track_number - 1)
  if not row then
    ack_embed(interaction, embed(string.format("\xE2\x9D\x8C Could not find track #%d in their history.", track_number), { color = COLOR.red }), true)
    return
  end
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  enqueue_track(gid, row.video_url, row.title, uid)
  local name = resolve_requester_name(gid, member_id)
  ack_embed(interaction, embed(string.format("Added **%s** to the queue from %s's history.", row.title, name), { title = "\xF0\x9F\xA5\xB7 Song Stolen!", color = COLOR.green }), false)
  if not playback[gid] then
    local channel_id = get_home_channel_id(gid)
    if channel_id then copas.addthread(function() process_queue(gid, channel_id) end) end
  end
end)

bot:command(cmdname("grab"), { description = "Send yourself the currently playing track in a direct message for easy saving or sharing." },
function(_, interaction)
  defer(interaction, true)
  local gid = guild_id_of(interaction)
  local data = playback[gid]
  if not data then followup_embed(interaction, embed("Nothing is currently playing.", { color = COLOR.red })); return end
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  local display = member_display_name(interaction.member)
  local dm = bot.rest:post("/users/@me/channels", { recipient_id = uid })
  local sent = false
  if dm and dm.id then
    local msg = bot.rest:post(("/channels/%s/messages"):format(dm.id), { embeds = { {
      title = "\xF0\x9F\x8E\xB5 Track Saved!",
      description = string.format("Hey **%s**!\nHere is the track you wanted to save:\n\n**[%s](%s)**", display, data.title or "Unknown Title", data.url),
      color = 0x5865F2,
    } } })
    sent = msg ~= nil
  end
  if sent then
    followup_embed(interaction, embed("\xF0\x9F\x93\xAC Check your DMs!", { color = COLOR.green }))
  else
    followup_embed(interaction, embed("\xE2\x9D\x8C I can't DM you! Please check your privacy settings.", { color = COLOR.red }))
  end
end)

bot:command(cmdname("like"), { description = "Teach Auto-DJ to play more tracks like the current one for you." },
function(_, interaction)
  local gid = guild_id_of(interaction)
  local snap = get_current_track_snapshot(gid)
  if not snap then ack(interaction, "Nothing is playing right now.", true); return end
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  record_track_feedback(gid, uid, snap.url, snap.title, true)
  ack_embed(interaction, embed("Saved your like for **" .. (snap.title or "this track") .. "**. Auto-DJ will lean toward similar picks.", { color = COLOR.green }), true)
end)

bot:command(cmdname("dislike"), { description = "Teach Auto-DJ to avoid the current track for your future recommendations." },
function(_, interaction)
  local gid = guild_id_of(interaction)
  local snap = get_current_track_snapshot(gid)
  if not snap then ack(interaction, "Nothing is playing right now.", true); return end
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  record_track_feedback(gid, uid, snap.url, snap.title, false)
  ack_embed(interaction, embed("Saved your dislike for **" .. (snap.title or "this track") .. "**. Auto-DJ will avoid it for you.", { color = COLOR.orange }), true)
end)

bot:command(cmdname("recommend"), {
  description = "Queue a smart recommendation learned from server taste, playlists, and listener feedback.",
  options = { { type = OPT.USER, name = "member", description = "Base the pick on this member's taste instead of yours", required = false } },
}, function(_, interaction)
  defer(interaction, false)
  local gid = guild_id_of(interaction)
  local requester_uid = interaction.member and interaction.member.user and interaction.member.user.id
  local target_id = get_opt(interaction, "member") or requester_uid
  local channel_id = get_home_channel_id(gid)
  if not channel_id then
    followup_embed(interaction, embed("Join a channel first or set a home channel.", { title = "Source Error", color = COLOR.red })); return
  end
  local chosen, reason = pick_smart_recommendation(gid, target_id)
  if not chosen then
    followup_embed(interaction, embed("I could not find a recommendation yet. Play a few tracks or save a playlist first.", { color = COLOR.red })); return
  end
  enqueue_track(gid, chosen.uri, chosen.title, requester_uid)
  q([[INSERT INTO tunestream_smart_recommendations (guild_id, requester_id, seed_title, seed_url, query_text, chosen_url, chosen_title, reason, accepted, created_at)
      VALUES (%s, %s, NULL, NULL, NULL, %s, %s, %s, true, NOW())]], gid, requester_uid, chosen.uri, chosen.title, reason)
  local qlen = queue_count(gid)
  local title
  if not playback[gid] then
    copas.addthread(function() process_queue(gid, channel_id) end)
    title = "Smart Recommendation Starting"
  else
    title = "Smart Recommendation Queued"
  end
  followup_embed(interaction, embed(string.format("Added **%s** based on **%s**. Queue size: %d", chosen.title, reason, qlen), { title = title, color = COLOR.green }))
end)

bot:command(cmdname("taste"), {
  description = "Show the saved taste profile Auto-DJ has learned for you or another member.",
  options = { { type = OPT.USER, name = "member", description = "Member", required = false } },
}, function(_, interaction)
  local gid = guild_id_of(interaction)
  local uid = interaction.member and interaction.member.user and interaction.member.user.id
  local target_id = get_opt(interaction, "member") or uid
  local rows, agg = build_user_taste_summary(gid, target_id)
  local name = resolve_requester_name(gid, target_id)
  if #rows == 0 then ack(interaction, "No saved taste profile for " .. name .. " yet.", true); return end
  local lines = {}
  for i = 1, math.min(8, #rows) do
    lines[#lines + 1] = string.format("**%d.** %s *(score %.1f)*", i, clean_smart_title(rows[i].title) or "Unknown Track", rows[i].score or 0)
  end
  local e = embed(table.concat(lines, "\n"), { title = "TUNESTREAM Taste Profile: " .. name, color = COLOR.blurple })
  agg = agg or {}
  e.fields = { { name = "Signals", value = string.format("plays %d | finishes %d | likes %d | dislikes %d | skips %d",
    agg.played or 0, agg.finished or 0, agg.liked or 0, agg.disliked or 0, agg.skipped or 0), inline = false } }
  ack_embed(interaction, e, true)
end)

-- ---- Audio & filters ----

bot:command(cmdname("volume"), {
  description = "Set the playback volume for this server from 1 to 200 percent.",
  options = { { type = OPT.INTEGER, name = "vol", description = "Volume percent", required = true, min_value = 1, max_value = 200 } },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local vol = clamp(get_opt(interaction, "vol"), 1, 200)
  fade_tokens[gid] = (fade_tokens[gid] or 0) + 1 -- cancel any in-flight fade
  ensure_guild_settings(gid)
  q("UPDATE tunestream_guild_settings SET volume = %s WHERE guild_id = %s", vol, gid)
  if playback[gid] then bot.lavalink:set_volume(gid, vol); playback[gid].volume = vol end
  ack_embed(interaction, embed(string.format("\xF0\x9F\x94\x8A Volume set to %d%%", vol), { color = COLOR.green }), true)
end)

bot:command(cmdname("loop"), {
  description = "Choose whether playback loops nothing, the current song, or the full queue.",
  options = { { type = OPT.STRING, name = "mode", description = "Loop mode", required = true,
    choices = { { name = "Off", value = "off" }, { name = "Song", value = "song" }, { name = "Queue", value = "queue" } } } },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local mode = get_opt(interaction, "mode")
  ensure_guild_settings(gid)
  q("UPDATE tunestream_guild_settings SET loop_mode = %s WHERE guild_id = %s", mode, gid)
  ack_embed(interaction, embed("\xF0\x9F\x94\x81 Looping set to: " .. mode, { color = COLOR.green }), true)
end)

bot:command(cmdname("filter"), {
  description = "Apply an audio filter such as nightcore, vaporwave, or bass boost to upcoming playback.",
  options = {
    { type = OPT.STRING, name = "mode", description = "Choose an audio filter to apply", required = true, choices = FILTER_PRESET_CHOICES },
    { type = OPT.STRING, name = "stack", description = "Optional second filter to layer on top (e.g. bassboost + nightcore)", required = false, choices = FILTER_PRESET_CHOICES },
  },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local mode = get_opt(interaction, "mode")
  local stack = get_opt(interaction, "stack")
  defer(interaction, true)
  ensure_guild_settings(gid)
  local stack_value = (stack == nil or stack == "none") and nil or stack
  if stack == nil then
    q("UPDATE tunestream_guild_settings SET filter_mode = %s, custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s", mode, gid)
  else
    q("UPDATE tunestream_guild_settings SET filter_mode = %s, filter_stack = %s, custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s", mode, stack_value, gid)
  end
  if playback[gid] then
    local filters = {}
    local speed = apply_filter_preset(filters, mode, 1.0)
    if stack_value and stack_value ~= mode then speed = apply_filter_preset(filters, stack_value, speed) end
    bot.lavalink:update_player(gid, { filters = filters })
    playback[gid].speed = speed
    playback[gid].filter_mode = mode
  end
  local label = FILTER_PRESET_VALUES[mode] or mode
  if stack_value and stack_value ~= mode then
    local stack_label = FILTER_PRESET_VALUES[stack_value] or stack_value
    followup_embed(interaction, embed(string.format("\xF0\x9F\x8E\x9B\xEF\xB8\x8F Filter set to: **%s** + **%s**.", label, stack_label), { color = COLOR.blurple }))
  else
    followup_embed(interaction, embed("\xF0\x9F\x8E\x9B\xEF\xB8\x8F Filter set to: **" .. label .. "**.", { color = COLOR.blurple }))
  end
end)

bot:command(cmdname("fade"), {
  description = "Customize track fade transitions, let the bot pick smart fade timing, or enable BPM-matched mixing.",
  options = {
    { type = OPT.STRING, name = "mode", description = "Fade mode", required = true, choices = {
      { name = "Smart Adaptive Fades", value = "smart" }, { name = "Custom Fades", value = "fade" },
      { name = "Beat-Matched Mixing", value = "mix" }, { name = "Disable Fades", value = "off" } } },
    { type = OPT.NUMBER, name = "seconds", description = "Fade length in seconds, from 0.5 to 20", required = false },
    { type = OPT.STRING, name = "curve", description = "Volume curve", required = false, choices = {
      { name = "Linear", value = "linear" }, { name = "Smooth", value = "smooth" },
      { name = "Slow Start", value = "ease_in" }, { name = "Soft Land", value = "ease_out" } } },
  },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local mode = get_opt(interaction, "mode")
  local seconds = clamp(get_opt(interaction, "seconds") or 3.0, 0.25, 12.0)
  local curve = get_opt(interaction, "curve") or "smooth"
  defer(interaction, true)
  ensure_guild_settings(gid)
  q("UPDATE tunestream_guild_settings SET transition_mode = %s, fade_seconds = %s, fade_curve = %s WHERE guild_id = %s", mode, seconds, curve, gid)
  local message, color
  if mode == "smart" then
    message, color = "\xF0\x9F\x8C\x8A Smart fades enabled. I will use short, smooth ramps that adapt to track length and active filters.", COLOR.green
  elseif mode == "fade" then
    message, color = string.format("\xF0\x9F\x8C\x8A Custom fades enabled: %gs using %s.", seconds, curve:gsub("_", " ")), COLOR.green
  elseif mode == "mix" then
    message, color = "\xF0\x9F\x8E\x9A\xEF\xB8\x8F Beat-matched mixing enabled (falls back to smart fades in this build -- BPM analysis was not ported; see the port's README notes).", COLOR.green
  else
    message, color = "\xE2\x8F\xB9\xEF\xB8\x8F Smooth fades disabled.", COLOR.red
  end
  followup_embed(interaction, embed(message, { color = color }))
end)

bot:command(cmdname("modify"), {
  description = "Apply temporary custom speed and pitch modifiers to the next few tracks in the queue.",
  options = {
    { type = OPT.NUMBER, name = "speed", description = "Speed multiplier (0.5 to 2.0)", required = false },
    { type = OPT.NUMBER, name = "pitch", description = "Pitch multiplier (0.5 to 2.0)", required = false },
    { type = OPT.INTEGER, name = "duration", description = "How many tracks this lasts (default 1)", required = false, min_value = 1 },
  },
}, function(_, interaction)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  local speed = clamp(get_opt(interaction, "speed") or 1.0, 0.5, 2.0)
  local pitch = clamp(get_opt(interaction, "pitch") or 1.0, 0.5, 2.0)
  local duration = get_opt(interaction, "duration") or 1
  defer(interaction, true)
  ensure_guild_settings(gid)
  local s = get_settings(gid)
  if s.filter_mode ~= "none" then
    followup_embed(interaction, embed("\xE2\x9D\x8C **Conflict:** Disable standard Filters via /tunestream_main_filter none first.", { color = COLOR.red }))
    return
  end
  q("UPDATE tunestream_guild_settings SET custom_speed = %s, custom_pitch = %s, custom_modifiers_left = %s WHERE guild_id = %s", speed, pitch, duration, gid)
  if playback[gid] then
    bot.lavalink:update_player(gid, { filters = { timescale = { speed = speed, pitch = pitch } } })
    playback[gid].speed = speed
  end
  followup_embed(interaction, embed(string.format("**Speed:** %sx\n**Pitch:** %sx\n*Active for the next %d track(s).*", speed, pitch, duration),
    { title = "\xF0\x9F\x8E\x9B\xEF\xB8\x8F Audio Modifiers Set", color = COLOR.gold }))
end)

-- ---- Scrubbing ----

local function do_seek(interaction, compute_target, response_text)
  if not is_dj(interaction) then return end
  local gid = guild_id_of(interaction)
  if not playback[gid] then ack(interaction, "Nothing playing.", true); return end
  local target = compute_target()
  bot.lavalink:seek(gid, math.max(0, target) * 1000)
  playback[gid].offset = math.max(0, target)
  playback[gid].start_time = socket.gettime()
  ack_embed(interaction, embed(response_text, { color = COLOR.green }), true)
end

bot:command(cmdname("seek"), {
  description = "Jump to an exact time in the current track using seconds from the start.",
  options = { { type = OPT.INTEGER, name = "seconds", description = "Seconds", required = true, min_value = 0 } },
}, function(_, interaction)
  local s = get_opt(interaction, "seconds")
  do_seek(interaction, function() return s end, "Seeked to " .. s .. "s")
end)

bot:command(cmdname("forward"), {
  description = "Jump forward within the current track by the number of seconds you provide.",
  options = { { type = OPT.INTEGER, name = "seconds", description = "Seconds", required = true, min_value = 1 } },
}, function(_, interaction)
  local s = get_opt(interaction, "seconds")
  do_seek(interaction, function() return current_position(guild_id_of(interaction)) + s end, "Skipped forward " .. s .. "s")
end)

bot:command(cmdname("rewind"), {
  description = "Jump backward within the current track by the number of seconds you provide.",
  options = { { type = OPT.INTEGER, name = "seconds", description = "Seconds", required = true, min_value = 1 } },
}, function(_, interaction)
  local s = get_opt(interaction, "seconds")
  do_seek(interaction, function() return math.max(0, current_position(guild_id_of(interaction)) - s) end, "Rewound " .. s .. "s")
end)

bot:command(cmdname("replay"), { description = "Restart the current track from the beginning without changing the queue." },
function(_, interaction)
  do_seek(interaction, function() return 0 end, "Replaying song.")
end)

-- ---- Panel ----

local function build_panel_embed(gid)
  local s = get_settings(gid)
  local e = embed(nil, { title = "\xF0\x9F\x8E\x9B\xEF\xB8\x8F TUNESTREAM Music Control Panel", color = 0x2B2D31 })
  local data = playback[gid]
  e.fields = {}
  if data then
    local cur = current_position(gid)
    local bar = progress_bar(cur, math.floor(data.duration or 0))
    local now_playing = data.url and string.format("**[%s](%s)**", data.title or "Playing", data.url) or ("**" .. (data.title or "Playing") .. "**")
    e.fields[#e.fields + 1] = { name = "Now Playing", value = now_playing .. "\n`" .. bar .. "`", inline = false }
    if data.requester_id then
      e.fields[#e.fields + 1] = { name = "Requested by", value = resolve_requester_name(gid, data.requester_id), inline = true }
    end
    e.fields[#e.fields + 1] = { name = "Filter", value = tostring(data.filter_mode or "none"), inline = true }
  else
    e.description = "Nothing is playing right now."
  end
  e.fields[#e.fields + 1] = { name = "Volume", value = s.volume .. "%", inline = true }
  e.fields[#e.fields + 1] = { name = "Loop", value = tostring(s.loop_mode), inline = true }
  e.footer = { text = "TUNESTREAM Main Music System" }
  return e
end

local PANEL_COMPONENTS = {
  { type = 1, components = {
    { type = 2, style = 1, label = "\xE2\x8F\xAF\xEF\xB8\x8F Play/Pause", custom_id = "tunestream:panel:playpause" },
    { type = 2, style = 4, label = "\xE2\x8F\xB9\xEF\xB8\x8F Stop", custom_id = "tunestream:panel:stop" },
    { type = 2, style = 2, label = "\xE2\x8F\xAD\xEF\xB8\x8F Skip", custom_id = "tunestream:panel:skip" },
  } },
  { type = 1, components = {
    { type = 2, style = 2, label = "\xE2\x8F\xAA -10s", custom_id = "tunestream:panel:rw" },
    { type = 2, style = 2, label = "\xE2\x8F\xA9 +10s", custom_id = "tunestream:panel:fw" },
    { type = 2, style = 2, label = "\xF0\x9F\x94\x89 Vol-", custom_id = "tunestream:panel:voldown" },
    { type = 2, style = 2, label = "\xF0\x9F\x94\x8A Vol+", custom_id = "tunestream:panel:volup" },
  } },
  { type = 1, components = {
    { type = 2, style = 2, label = "\xF0\x9F\x94\x81 Loop", custom_id = "tunestream:panel:looptoggle" },
    { type = 2, style = 3, label = "\xF0\x9F\x94\x80 Shuffle", custom_id = "tunestream:panel:shuffle" },
    { type = 2, style = 2, label = "\xF0\x9F\x93\x9C View Queue", custom_id = "tunestream:panel:queue" },
  } },
}

bot:command(cmdname("panel"), { description = "Post an interactive control panel with playback, queue, and transport buttons." },
function(_, interaction)
  -- ack() only supports plain content/embeds; panel needs `components` too, so build the
  -- INTERACTION_CALLBACK payload directly here instead.
  bot.rest:post(("/interactions/%s/%s/callback"):format(interaction.id, interaction.token),
    { type = 4, data = { embeds = { build_panel_embed(guild_id_of(interaction)) }, components = PANEL_COMPONENTS } })
end)

bot:command(cmdname("lyrics"), { description = "Show lyrics for the currently playing track, synced to the current position when available." },
function(_, interaction)
  local gid = guild_id_of(interaction)
  local data = playback[gid]
  if not data then ack_embed(interaction, embed("Nothing is playing.", { color = COLOR.red }), false); return end
  defer(interaction, false)
  local title = data.title or "Unknown"
  local result = fetch_lyrics(title, data.duration)
  if not result then
    followup_embed(interaction, embed("No lyrics found for **" .. title .. "**.", { color = COLOR.red }))
    return
  end
  local e = embed(nil, { title = "\xF0\x9F\x93\x9C " .. (result.track_name or title), color = COLOR.blue })
  if result.artist_name then e.author = { name = result.artist_name } end
  if result.synced then
    local pos = current_position(gid)
    local cur_idx = 1
    for i, line in ipairs(result.synced) do
      if line.ts <= pos then cur_idx = i else break end
    end
    local lo = math.max(1, cur_idx - 2)
    local hi = math.min(#result.synced, cur_idx + 4)
    local body = {}
    for i = lo, hi do
      local text = result.synced[i].text
      body[#body + 1] = (i == cur_idx) and ("**" .. text .. "**") or text
    end
    e.description = table.concat(body, "\n"):sub(1, 4000)
    e.footer = { text = "Synced to current playback position — rerun to refresh." }
  else
    e.description = (result.plain or "No lyrics text available."):sub(1, 4000)
  end
  followup_embed(interaction, e)
end)

bot:command(cmdname("nowplaying"), { description = "Show the current track, progress bar, requester, and live playback status." },
function(_, interaction)
  local gid = guild_id_of(interaction)
  local data = playback[gid]
  if not data then ack_embed(interaction, embed("Nothing playing.", { color = COLOR.red }), true); return end
  local bar = progress_bar(current_position(gid), math.floor(data.duration or 0))
  local e = embed(string.format("**[%s](%s)**\n\n`%s`", data.title or "Playing", data.url, bar), { title = "\xF0\x9F\x8E\xB5 Now Playing", color = COLOR.blue })
  e.fields = { { name = "Filter", value = tostring(data.filter_mode or "none"), inline = true } }
  if data.requester_id then
    table.insert(e.fields, 1, { name = "Requested by", value = resolve_requester_name(gid, data.requester_id), inline = true })
  end
  ack_embed(interaction, e, true)
end)

bot:command(cmdname("ping"), { description = "Show the bot websocket latency so you can quickly check responsiveness." },
function(_, interaction)
  ack_embed(interaction, embed("\xF0\x9F\x8F\x93 Pong!", { color = COLOR.green }), true)
end)

local START_TIME = socket.gettime()
bot:command(cmdname("uptime"), { description = "Show how long this bot process has been running since its last startup." },
function(_, interaction)
  local secs = math.floor(socket.gettime() - START_TIME)
  local h, m, s = math.floor(secs / 3600), math.floor((secs % 3600) / 60), secs % 60
  ack_embed(interaction, embed(string.format("\xE2\x8F\xB1\xEF\xB8\x8F Uptime: %d:%02d:%02d", h, m, s), { color = COLOR.green }), true)
end)

bot:command(cmdname("stats"), { description = "Show quick bot statistics such as guild count and active player count." },
function(_, interaction)
  local players = 0
  for _ in pairs(playback) do players = players + 1 end
  ack_embed(interaction, embed(string.format("\xF0\x9F\x93\x8A Active Players: %d", players), { color = COLOR.green }), true)
end)

local ALL_COMMAND_NAMES = {}

bot:command(cmdname("help"), { description = "Show a categorized help menu for all TUNESTREAM music commands and utilities" },
function(_, interaction)
  ack_embed(interaction, embed(table.concat(ALL_COMMAND_NAMES, ", "), { title = "\xF0\x9F\x93\x9A Command List", color = COLOR.blue }), true)
end)

for name in pairs(bot.commands) do table.insert(ALL_COMMAND_NAMES, name) end
table.sort(ALL_COMMAND_NAMES)

-- ===========================================================================
-- Autocomplete + message-component (panel button) routing.
-- These are extra handlers on the same gateway event bot.lua already listens
-- to for slash-command dispatch (type 2); Gateway:on supports multiple
-- handlers per event, so this doesn't touch swarmlua/bot.lua at all.
-- ===========================================================================

local AUTOCOMPLETE_INDEX_COMMANDS = {
  [cmdname("remove")] = true, [cmdname("skipto")] = true, [cmdname("move")] = true, [cmdname("bump")] = true,
}

bot.gateway:on("INTERACTION_CREATE", function(interaction)
  if interaction.type == 4 then -- APPLICATION_COMMAND_AUTOCOMPLETE
    local name = interaction.data and interaction.data.name
    local focused = focused_opt(interaction)
    if not focused then autocomplete_result(interaction, {}); return end
    local ok, choices = pcall(function()
      if name == cmdname("play") and focused.name == "search" then
        local current = trim(focused.value or "")
        local suggestions = {}
        if #current >= 2 and not is_explicit_lavalink_query(current) then
          suggestions = youtube_suggest(current)
        end
        if #suggestions == 0 and current ~= "" then suggestions = { current } end
        local out = {}
        for i, s in ipairs(suggestions) do
          if i > 10 then break end
          out[#out + 1] = { name = s:sub(1, 100), value = s:sub(1, 100) }
        end
        return out
      elseif AUTOCOMPLETE_INDEX_COMMANDS[name] and (focused.name == "index" or focused.name == "frm" or focused.name == "to") then
        return queue_index_choices(guild_id_of(interaction), focused.value)
      end
      return {}
    end)
    autocomplete_result(interaction, ok and choices or {})
    return
  end

  if interaction.type == 3 then -- MESSAGE_COMPONENT (panel buttons)
    local cid = interaction.data and interaction.data.custom_id or ""
    local action = cid:match("^tunestream:panel:(.+)$")
    if not action then return end
    local gid = guild_id_of(interaction)

    -- bot.lua's own on_error hook only covers type=2 (APPLICATION_COMMAND)
    -- handler failures; this whole branch is a second, bot-file-owned
    -- listener on the same gateway event (see comment above bot.on_error),
    -- so wrap it in its own pcall + report_error the same way
    -- nexus.lua/sapphire.lua's panel handlers do, or button failures would
    -- never reach tunestream_error_events / the error webhook.
    local panel_ok, panel_err = pcall(function()

    local function refresh() update_message(interaction, build_panel_embed(gid), PANEL_COMPONENTS) end

    if action == "playpause" then
      if not is_dj(interaction) then return end
      local data = playback[gid]
      if data then
        if data.paused then
          bot.lavalink:set_paused(gid, false); data.start_time = socket.gettime(); data.paused = false
        else
          bot.lavalink:set_paused(gid, true); data.offset = current_position(gid); data.paused = true
        end
        refresh()
      else
        ack(interaction, "Nothing is playing.", true)
      end
    elseif action == "stop" then
      if not is_dj(interaction) then return end
      stop_playback(gid)
      refresh()
    elseif action == "skip" then
      if not is_dj(interaction) then return end
      if playback[gid] then bot.lavalink:stop(gid); ack(interaction, "\xE2\x8F\xAD\xEF\xB8\x8F Skipped to next track", true)
      else ack(interaction, "Nothing to skip.", true) end
    elseif action == "rw" or action == "fw" then
      if not is_dj(interaction) then return end
      if not playback[gid] then ack(interaction, "Nothing playing.", true); return end
      local delta = (action == "rw") and -10 or 10
      local new_pos = math.max(0, current_position(gid) + delta)
      bot.lavalink:seek(gid, new_pos * 1000)
      playback[gid].offset = new_pos; playback[gid].start_time = socket.gettime()
      refresh()
    elseif action == "voldown" or action == "volup" then
      if not is_dj(interaction) then return end
      local s = get_settings(gid)
      local vol = clamp(s.volume + ((action == "volup") and 10 or -10), 1, 200)
      q("UPDATE tunestream_guild_settings SET volume = %s WHERE guild_id = %s", vol, gid)
      if playback[gid] then bot.lavalink:set_volume(gid, vol); playback[gid].volume = vol end
      refresh()
    elseif action == "looptoggle" then
      if not is_dj(interaction) then return end
      local s = get_settings(gid)
      local next_mode = ({ off = "queue", queue = "song", song = "off" })[s.loop_mode] or "queue"
      q("UPDATE tunestream_guild_settings SET loop_mode = %s WHERE guild_id = %s", next_mode, gid)
      refresh()
    elseif action == "shuffle" then
      if not is_dj(interaction) then return end
      local n = shuffle_queue(gid, false)
      if n <= 0 then ack(interaction, "Queue empty.", true); return end
      snapshot_backup(gid)
      ack(interaction, "\xF0\x9F\x94\x80 Queue successfully shuffled!", true)
    elseif action == "queue" then
      local rows = q("SELECT title FROM tunestream_queue WHERE guild_id = %s AND bot_name = 'tunestream' ORDER BY id ASC LIMIT 10", gid)
      if #rows == 0 then ack(interaction, "Queue is empty.", true); return end
      local lines = { "**Current Queue:**" }
      for i, r in ipairs(rows) do lines[#lines + 1] = string.format("%d. %s", i, r.title) end
      ack(interaction, table.concat(lines, "\n"), true)
    end
    end) -- panel_ok pcall
    if not panel_ok then
      log_error("panel button handler error: %s", tostring(panel_err))
      report_error(gid, "command_error", "panel:" .. tostring(action), tostring(panel_err))
    end
  end
end)

-- ===========================================================================
-- Background maintenance: position/state persistence + swarm heartbeat.
-- A much smaller cousin of tunestream.py's dozen tasks.loop()s -- this LuaJIT
-- stack is single-process/blocking-REST so most of those existed to paper
-- over asyncio/aiomysql-pool races that don't apply here.
-- ===========================================================================

-- Factored into named functions (rather than left as anonymous copas-thread
-- bodies like rhythm.lua) so db_smoke_test.lua can call heartbeat_tick()
-- directly and exercise the tunestream_metrics %s-placeholder-count/arg-count
-- shape and the swarm_health upsert without waiting on a real 30s timer.
local function persist_positions_tick()
  for gid, data in pairs(playback) do
    if not data.paused then
      local ok = pcall(function()
        q([[UPDATE tunestream_playback_state SET position_seconds = %s, last_checkpoint_at = NOW()
            WHERE guild_id = %s AND bot_name = 'tunestream']], current_position(gid), gid)
      end)
      if not ok then log_warn("[%s] position persist failed", gid) end
    end
  end
end

local function heartbeat_tick()
  q([[INSERT INTO swarm_health (bot_name, last_pulse, status) VALUES ('tunestream', NOW(), 'online')
      ON CONFLICT (bot_name) DO UPDATE SET last_pulse = NOW(), status = 'online']])
  for gid, data in pairs(playback) do
    q([[INSERT INTO tunestream_metrics (guild_id, bot_name, voice_connected, player_connected, player_playing, player_paused,
          queue_count, backup_queue_count, is_playing_db, is_paused_db, position_seconds, duration_seconds, lavalink_ready, updated_at)
        VALUES (%s, 'tunestream', true, true, %s, %s, %s, 0, %s, %s, %s, %s, true, NOW())
        ON CONFLICT (guild_id, bot_name) DO UPDATE SET voice_connected = EXCLUDED.voice_connected,
          player_connected = EXCLUDED.player_connected, player_playing = EXCLUDED.player_playing,
          player_paused = EXCLUDED.player_paused, queue_count = EXCLUDED.queue_count,
          is_playing_db = EXCLUDED.is_playing_db, is_paused_db = EXCLUDED.is_paused_db,
          position_seconds = EXCLUDED.position_seconds, duration_seconds = EXCLUDED.duration_seconds, updated_at = NOW()]],
      gid, not data.paused, data.paused, queue_count(gid), not data.paused, data.paused, current_position(gid), math.floor(data.duration or 0))
  end
end

-- SwarmPanel/Aria remote-control bridge -- was entirely missing, so the
-- panel's PAUSE/RESUME/SKIP/STOP/RESTART buttons silently did nothing for
-- this bot. Same poll-and-execute-then-delete pattern as alucard.lua/
-- gws.lua/sapphire.lua.
local function poll_swarm_overrides()
  local rows = q("SELECT guild_id, command FROM tunestream_swarm_overrides WHERE bot_name = 'tunestream'") or {{}}
  for _, row in ipairs(rows) do
    local guild_id = tostring(row.guild_id)
    local cmd_name = (row.command or ""):upper()
    local executed = false
    if cmd_name == "RESTART" then
      q("DELETE FROM tunestream_swarm_overrides WHERE guild_id = %s AND bot_name = 'tunestream'", guild_id)
      send_webhook_log("\240\159\164\150 Aria Override", "Aria requested a restart.", COLOR.purple)
      os.exit(0)
    elseif cmd_name == "PAUSE" and playback[guild_id] and not playback[guild_id].paused then
      playback[guild_id].paused = true
      playback[guild_id].offset = current_position(guild_id)
      bot.lavalink:set_paused(guild_id, true)
      executed = true
    elseif cmd_name == "RESUME" and playback[guild_id] and playback[guild_id].paused then
      playback[guild_id].paused = false
      playback[guild_id].start_time = socket.gettime()
      bot.lavalink:set_paused(guild_id, false)
      executed = true
    elseif cmd_name == "SKIP" and playback[guild_id] then
      bot.lavalink:stop(guild_id)
      executed = true
    elseif cmd_name == "STOP" then
      stop_playback(guild_id)
      executed = true
    end
    if executed then
      send_webhook_log("\240\159\164\150 Aria Override", ("Aria executed **%s** in guild %s."):format(cmd_name, guild_id), COLOR.purple)
    end
    q("DELETE FROM tunestream_swarm_overrides WHERE guild_id = %s AND bot_name = 'tunestream' AND command = %s", guild_id, row.command)
  end
end

-- SwarmPanel's control.lua also writes uppercase PLAY/RECOVER/LEAVE/SEEK
-- rows to tunestream_swarm_direct_orders for source_url plays, fleet-
-- supervisor recovery, forced disconnects, and seek-to-position -- none of
-- the simpler overrides poller above covers these. Same claim-then-execute
-- pattern.
local function handle_direct_order(order)
  local guild_id = tostring(order.guild_id)
  local cmd = order.command
  local ok, err = pcall(function()
    if cmd == "PLAY" and order.data and order.vc_id and order.vc_id ~= "0" then
      local entries, _pn, terr = search_playables(order.data)
      if not entries or #entries == 0 then error("could not resolve source: " .. tostring(terr), 0) end
      for _, e in ipairs(entries) do
        enqueue_track(guild_id, e.uri, e.title, nil)
      end
      if #entries > 1 then shuffle_queue(guild_id, true) end
      snapshot_backup(guild_id)
      if not playback[guild_id] then process_queue(guild_id, order.vc_id) end
    elseif cmd == "RECOVER" then
      local channel_id = (order.vc_id and order.vc_id ~= "0" and order.vc_id) or get_home_channel_id(guild_id)
      if channel_id and not playback[guild_id] then process_queue(guild_id, channel_id) end
    elseif cmd == "LEAVE" then
      stop_playback(guild_id)
      bot.gateway:leave_voice(guild_id)
    elseif cmd == "SEEK" and order.data then
      local target_seconds = math.max(0, tonumber(order.data) or 0)
      if playback[guild_id] then
        bot.lavalink:seek(guild_id, target_seconds * 1000)
        playback[guild_id].offset = target_seconds
        playback[guild_id].start_time = socket.gettime()
      end
    end
  end)
  if not ok then
    q("UPDATE tunestream_swarm_direct_orders SET attempts = attempts + 1, last_error = %s WHERE id = %s", tostring(err):sub(1, 500), order.id)
  else
    q("DELETE FROM tunestream_swarm_direct_orders WHERE id = %s", order.id)
    send_webhook_log("\240\159\164\150 Aria Direct Order", ("Aria executed **%s** in guild %s."):format(cmd, guild_id), COLOR.purple)
  end
end

local function poll_direct_orders()
  local rows = q("SELECT id, guild_id, vc_id, text_channel_id, command, data FROM tunestream_swarm_direct_orders WHERE bot_name = 'tunestream' AND claimed_at IS NULL ORDER BY id ASC LIMIT 5") or {}
  for _, order in ipairs(rows) do
    q("UPDATE tunestream_swarm_direct_orders SET claimed_at = NOW() WHERE id = %s", order.id)
    handle_direct_order(order)
  end
end

local function start_background_loops()
  copas.addthread(function()
    while true do
      copas.sleep(10)
      persist_positions_tick()
    end
  end)

  copas.addthread(function()
    while true do
      copas.sleep(10)
      local ok, err = pcall(poll_swarm_overrides)
      if not ok then
        log_warn("swarm override poll error: %s", tostring(err))
        report_error(nil, "runtime", "swarm override poll error", tostring(err))
      end
    end
  end)

  copas.addthread(function()
    while true do
      copas.sleep(5)
      local ok, err = pcall(poll_direct_orders)
      if not ok then
        log_warn("direct order poll error: %s", tostring(err))
        report_error(nil, "runtime", "direct order poll error", tostring(err))
      end
    end
  end)

  copas.addthread(function()
    while true do
      copas.sleep(PLAYLIST_SYNC_INTERVAL)
      local ok, err = pcall(playlist_sync_tick)
      if not ok then
        log_warn("playlist sync tick error: %s", tostring(err))
        report_error(nil, "runtime", "playlist sync tick error", tostring(err))
      end
    end
  end)

  copas.addthread(function()
    while true do
      copas.sleep(30)
      local ok, err = pcall(heartbeat_tick)
      if not ok then
        log_warn("heartbeat tick failed: %s", tostring(err))
        report_error(nil, "runtime", "heartbeat tick error", tostring(err))
      end
    end
  end)

  -- Mirrors tunestream.py's "Database Janitor" retention pass for error_events
  -- (30-day retention); everything else that task did (history pruning,
  -- cache cleanup) is covered by the documented simplifications up top.
  copas.addthread(function()
    while true do
      copas.sleep(6 * 3600)
      local ok, err = pcall(q, "DELETE FROM tunestream_error_events WHERE created_at < NOW() - INTERVAL '30 days'")
      if not ok then log_warn("error_events cleanup failed: %s", tostring(err)) end
    end
  end)
end

-- BUGFIX: the Discord gateway sends a fresh READY (not RESUMED) on every
-- invalidated session -- routine reconnects, not just process restarts --
-- and this handler used to redo its full boot-resume/background-loop-spawn
-- pass every single time, which both force-restarted/discarded queued
-- tracks on reconnect and piled up duplicate background loops. Gate on
-- did_initial_ready so it runs exactly once per process (matching what it was
-- actually written for).
local did_initial_ready = false

bot.gateway:on("READY", function()
  if did_initial_ready then return end
  did_initial_ready = true
  send_webhook_log("\xF0\x9F\x9F\xA2 Node Online", "TUNESTREAM is online and syncing with the swarm.", COLOR.green)
  -- Rejoin and resume for any guild that was playing (or had a non-empty
  -- queue) when this process last stopped -- container recreates/restarts
  -- otherwise leave the bot sitting disconnected with a full queue and no
  -- way back in short of a manual /play.
  copas.addthread(function()
    -- Wait for the Lavalink websocket to actually be connected before
    -- touching the queue -- process_queue deletes each row before resolving
    -- it, so racing this against Lavalink's own (often slow, cold-start)
    -- connect meant every "no response from Lavalink" failure permanently
    -- destroyed that queued track instead of just skipping playback.
    local waited = 0
    while not (bot.lavalink and bot.lavalink.session_id) and waited < 60 do
      copas.sleep(1)
      waited = waited + 1
    end
    local rows = q("SELECT DISTINCT guild_id FROM tunestream_bot_home_channels WHERE bot_name = 'tunestream'") or {}
    for _, row in ipairs(rows) do
      local gid = row.guild_id
      local channel_id = get_home_channel_id(gid)
      if channel_id then
        local has_queue = queue_count(gid) > 0
        local was_playing = q1("SELECT is_playing FROM tunestream_playback_state WHERE guild_id = %s AND bot_name = 'tunestream'", gid)
        if has_queue or (was_playing and was_playing.is_playing) then
          log_info("[%s] resuming on boot (queue=%s, was_playing=%s)", gid, tostring(has_queue), tostring(was_playing and was_playing.is_playing))
          ensure_voice_connection(gid, channel_id)
          process_queue(gid, channel_id)
        end
      end
    end
  end)
end)

-- ===========================================================================
-- Boot
-- ===========================================================================

init_db()

local function command_count()
  local n = 0
  for _ in pairs(bot.commands) do n = n + 1 end
  return n
end

-- Exposes internals for the DB/logic smoke test (db_smoke_test.lua); not
-- reached in normal operation (that script sets this env var itself). Mirrors
-- nexus.lua/sapphire.lua/symphony.lua's TUNESTREAM_DRY_RUN=test export pattern --
-- rhythm.lua (this file's structural template) had no equivalent, so it could
-- only be validated by hand rather than an automated live-DB smoke test.
if env("TUNESTREAM_DRY_RUN") == "test" then
  return {
    q = q, q1 = q1, col = col,
    ensure_guild_settings = ensure_guild_settings, get_settings = get_settings,
    get_home_channel_id = get_home_channel_id, get_autodj_enabled = get_autodj_enabled,
    set_autodj_enabled = set_autodj_enabled,
    enqueue_track = enqueue_track, insert_queue_front = insert_queue_front,
    snapshot_backup = snapshot_backup, restore_queue_from_backup = restore_queue_from_backup,
    shuffle_queue = shuffle_queue, queue_count = queue_count, delete_backup_track = delete_backup_track,
    track_key = track_key, record_track_feedback = record_track_feedback,
    pick_smart_recommendation = pick_smart_recommendation,
    clear_playback_state = clear_playback_state,
    mark_voice_connected = mark_voice_connected, mark_voice_disconnected = mark_voice_disconnected,
    persist_positions_tick = persist_positions_tick, heartbeat_tick = heartbeat_tick,
    report_error = report_error, persist_error_event = persist_error_event,
    playback = playback, bot = bot, command_count = command_count,
  }
elseif env("TUNESTREAM_DRY_RUN") then
  log_info("dry run OK: %d commands registered, db/lavalink/gateway objects constructed.", command_count())
  os.exit(0)
end

start_background_loops()
log_info("TUNESTREAM booting -- %d commands registered, Lavalink at %s:%d", command_count(), LL_HOST, LL_PORT)

bot:run()
