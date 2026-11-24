-- Release or expire a hold, returning its unit to the bucket.
--
-- KEYS[1] units list   KEYS[2] hold hash   KEYS[3] expiry zset
-- ARGV[1] reservation_id
-- ARGV[2] mode: 'release' (any state, used after a declined payment or unfreeze failure)
--               'expire'  (only HELD holds whose expiry is <= ARGV[3])
-- ARGV[3] now (unix ms, used by 'expire')
--
-- Returns {status, user_id}; status is RELEASED, EXPIRED or SKIP.

local h = redis.call('HMGET', KEYS[2], 'state', 'unit', 'user', 'exp')
local state, unit, user, exp = h[1], h[2], h[3], h[4]

if ARGV[2] == 'expire' then
  local score = redis.call('ZSCORE', KEYS[3], ARGV[1])
  if not score or tonumber(score) > tonumber(ARGV[3]) then
    return {'SKIP', ''}
  end
  redis.call('ZREM', KEYS[3], ARGV[1])
  if state ~= 'HELD' then
    return {'SKIP', ''}
  end
end

if not state then
  redis.call('ZREM', KEYS[3], ARGV[1])
  return {'SKIP', ''}
end

redis.call('RPUSH', KEYS[1], unit)
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[1])
if ARGV[2] == 'expire' then
  return {'EXPIRED', user}
end
return {'RELEASED', user}
