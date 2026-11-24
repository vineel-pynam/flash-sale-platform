-- Fallback path: take a unit from a non-home bucket and create the hold there.
-- The caller has already claimed the user's marker in the home bucket.
--
-- KEYS[1] units list   KEYS[2] hold hash   KEYS[3] expiry zset   (all in the same bucket)
-- ARGV[1] user_id      ARGV[2] reservation_id   ARGV[3] expires_at (unix ms)
--
-- Returns unit_id, or false when the bucket is empty.

local unit = redis.call('LPOP', KEYS[1])
if not unit then
  return false
end
redis.call('HSET', KEYS[2], 'user', ARGV[1], 'unit', unit, 'state', 'HELD', 'exp', ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
return unit
