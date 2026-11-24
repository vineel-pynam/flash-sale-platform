-- Put a frozen hold back on a timer (checkout failed before charging, or the checkout crashed).
-- Only a hold that is still FROZEN is touched: a hold that was completed, released or never existed
-- is left alone, so this can never create a partial hold.
--
-- KEYS[1] hold hash   KEYS[2] expiry zset
-- ARGV[1] reservation_id   ARGV[2] new expires_at (unix ms)
--
-- Returns 1 if the hold was unfrozen, 0 otherwise.

if redis.call('HGET', KEYS[1], 'state') ~= 'FROZEN' then
  return 0
end
redis.call('HSET', KEYS[1], 'state', 'HELD', 'exp', ARGV[2])
redis.call('HDEL', KEYS[1], 'frozen_at')
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 1
