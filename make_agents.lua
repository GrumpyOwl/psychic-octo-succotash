
redis.replicate_commands()
local MAX_NUMBER_OF_AGENTS=100000
local MAX_NUMBER_OF_FRIENDS=20

local time_start = redis.call("time")
for i=1, MAX_NUMBER_OF_AGENTS do
	local lat = (math.random()*(54.50-49))+49
	local lon = (math.random()*(24.09-14.07))+14.07

	redis.call(
	"HMSET", "consumer:"..i,
			"id", i,
            "importance_weight", string.format( "%#.2f", math.random()),
            "importance_budget", string.format( "%#.2f", math.random()),
            "importance_brand", string.format( "%#.2f", math.random()),
            "sensibility_online_ad", string.format( "%#.2f", math.random()),
            "sensibility_outdoor_ad", string.format( "%#.2f", math.random()),
            "sensibility_other_ad", string.format( "%#.2f", math.random())
		)

	redis.call(
	"ZADD", "consumer:"..i..":brand_preferences",
			((math.random()*(1+1))-1), "Lenovo",
            ((math.random()*(1+1))-1), "Dell",
            ((math.random()*(1+1))-1), "Asus"
		)

	redis.call("GEOADD", "Polska", lat, lon, i)

	for j=1, MAX_NUMBER_OF_FRIENDS do 
		redis.call(
			"ZADD", "consumer:"..i..":friends",
			   math.random(), math.random(1, MAX_NUMBER_OF_AGENTS)..""
		)
	end
end

local time_end = redis.call("time")
return string.format( "%#.2f", (time_end[1]+time_end[2]/1000000)-(time_start[1]+time_start[2]/1000000))
