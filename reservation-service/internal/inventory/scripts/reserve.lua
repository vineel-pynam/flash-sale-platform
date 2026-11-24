-- Atomically reserve one unit from the user's home bucket.
-- All keys share the bucket's hash tag, so this runs on a single Redis Cluster shard.
--
-- KEYS[1] units list      inv:{drop:bb}:units
-- KEYS[2] user marker     inv:{drop:bb}:user:<user_id>
-- KEYS[3] hold hash       inv:{drop:bb}:hold:<reservation_id>
-- KEYS[4] expiry zset     inv:{drop:bb}:expiry
-- ARGV[1] user_id
-- ARGV[2] reservation_id
-- ARGV[3] expires_at (unix ms)
-- ARGV[4] marker ttl (ms)
-- ARGV[5] pending-claim ttl (ms)
--
-- Returns {status, value}:
--   {'OK', unit_id}                   hold created
--   {'ALREADY_HELD', reservation_id}  user already holds or bought a unit (idempotent on retry)
--   {'BUSY', ''}                      another request for this user is in progress
--   {'EMPTY_CLAIMED', ''}             home bucket empty; marker claimed so the caller can try other buckets

local existing = redis.call('GET', KEYS[2])
if existing then
  if string.sub(existing, 1, 8) == 'pending:' then
    return {'BUSY', ''}
  end
  return {'ALREADY_HELD', existing}
end

local unit = redis.call('LPOP', KEYS[1])
if not unit then
  redis.call('SET', KEYS[2], 'pending:' .. ARGV[2], 'PX', ARGV[5])
  return {'EMPTY_CLAIMED', ''}
end

redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[4])
redis.call('HSET', KEYS[3], 'user', ARGV[1], 'unit', unit, 'state', 'HELD', 'exp', ARGV[3])
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[2])
return {'OK', unit}
