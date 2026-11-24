-- Mark a hold as sold after a successful payment. The unit is not returned.
--
-- KEYS[1] hold hash   KEYS[2] expiry zset
-- ARGV[1] reservation_id
--
-- Returns 1 if a hold was removed, 0 if it was already gone.

redis.call('ZREM', KEYS[2], ARGV[1])
return redis.call('DEL', KEYS[1])
