local mt = setmetatable(_G, nil)
redis.replicate_commands()
local MAX_NUMBER_OF_AGENTS=1000000

KEY_BRAND_PREFERENCES = ":consumer:brand_preferences"
KEY_CONSUMER = ":consumer"
KEY_CONSUMER_FRIENDS = ":consumer:friends"

MAX_NUMBER_OF_FRIENDS=20
time_start = redis.call("time")

for i=1, MAX_NUMBER_OF_AGENTS do
	local lat = (math.random()*(54.50-49))+49
	local lon = (math.random()*(24.09-14.07))+14.07

	redis.call(
	"HMSET", tostring(i)..KEY_CONSUMER,
			"id", i,
            "importance_weight", string.format( "%#.2f", math.random()),
            "importance_budget", string.format( "%#.2f", math.random()),
            "importance_brand", string.format( "%#.2f", math.random()),
            "sensibility_online_ad", string.format( "%#.2f", math.random()),
            "sensibility_outdoor_ad", string.format( "%#.2f", math.random()),
            "sensibility_other_ad", string.format( "%#.2f", math.random())
		)

	redis.call(
	"ZADD", tostring(i)..KEY_BRAND_PREFERENCES,
			((math.random()*(1+1))-1), "Lenovo",
            ((math.random()*(1+1))-1), "Dell",
            ((math.random()*(1+1))-1), "Asus"
		)

	redis.call("GEOADD", "Polska", lat, lon, i)

	for j=1, MAX_NUMBER_OF_FRIENDS do 
		redis.call(
			"ZADD", tostring(i)..KEY_CONSUMER_FRIENDS,
			   math.random(), math.random(1, MAX_NUMBER_OF_AGENTS)..""
		)
	end
end

local time_end = redis.call("time")
return string.format( "%#.2f", (time_end[1]+time_end[2]/1000000)-(time_start[1]+time_start[2]/1000000))
