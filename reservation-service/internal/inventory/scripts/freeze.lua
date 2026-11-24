-- Freeze a hold before payment starts: confirm it is still HELD and unexpired, then remove it from
-- the expiry set so the reaper cannot release it while payment is in flight.
-- Idempotent: freezing an already frozen hold succeeds.
--
-- KEYS[1] hold hash   KEYS[2] expiry zset
-- ARGV[1] reservation_id   ARGV[2] now (unix ms)
--
-- Returns {status, unit_id, user_id}; status is OK, NOT_FOUND or EXPIRED.

local h = redis.call('HMGET', KEYS[1], 'state', 'unit', 'user', 'exp')
local state, unit, user, exp = h[1], h[2], h[3], h[4]
if not state then
  return {'NOT_FOUND', '', ''}
end
if state == 'FROZEN' then
  return {'OK', unit, user}
end
if tonumber(exp) <= tonumber(ARGV[2]) then
  return {'EXPIRED', '', ''}
end
redis.call('HSET', KEYS[1], 'state', 'FROZEN', 'frozen_at', ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[1])
return {'OK', unit, user}
