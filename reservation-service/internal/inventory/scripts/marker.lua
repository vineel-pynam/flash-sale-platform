-- Compare-and-set on a user marker, so one request never overwrites another request's marker.
--
-- KEYS[1] user marker
-- ARGV[1] expected value
-- ARGV[2] action: 'set' | 'persist' | 'delete'
-- ARGV[3] new value (for 'set')
-- ARGV[4] ttl ms (for 'set'; 0 means no expiry)
--
-- Returns 1 when the marker held the expected value and the action was applied, otherwise 0.

if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
if ARGV[2] == 'set' then
  if tonumber(ARGV[4]) > 0 then
    redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
  else
    redis.call('SET', KEYS[1], ARGV[3])
  end
elseif ARGV[2] == 'persist' then
  redis.call('PERSIST', KEYS[1])
elseif ARGV[2] == 'delete' then
  redis.call('DEL', KEYS[1])
end
return 1
