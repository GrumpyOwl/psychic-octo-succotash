local mt = setmetatable(_G, nil) --moduł no Globals
redis.replicate_commands()
--Model 1-----------------------------------------------
redis.replicate_commands()
MAX_NUMBER_OF_AGENTS = 100000
MAX_NUMBER_OF_FRIENDS = 20 --liczba friendsów
NUMBER_NEIGHBOUR = 20 --ilość sąsiadów
NUMBER_OF_AGENTS_TO_UPDATE = MAX_NUMBER_OF_AGENTS * 0.1
NUMBER_OF_FRIENDS_TO_UPDATE = 0.5 * MAX_NUMBER_OF_FRIENDS --
NUMBER_OF_INTERVAL = 3
PERCETAGE_BASE_POPULATION_WHICH_BUY_LAPTOP = 0.2

TABLE_BRANDS = {"Dell", "Lenovo", "Asus"}--używane do losowania przy tworzeniu laptopa
NUMBER_LAPTOPS = 10
TRESHOLD_DECISION_MAKING = 0.9
KEY_CONSUMER = ":consumer"
KEY_BRAND_PREFERENCES = ":consumer:brand_preferences"
KEY_MATRIX_UTILITY = ":consumer:utility"
KEY_VECTOR_WHO_BY = "consumer:who_buy"
KEY_CONSUMER_FRIENDS = ":consumer:friends"
KEY_LAPTOP = ":laptop"
---------------------------------------------
toTable = function(table)
	local result = {}
	local key
	if (table == nil) then return -1 end
	for i, v in ipairs(table) do
		if i%2 == 1 then
			key = v
		else
			result[key] = v		
		end
	end
	return result
end
--lua script to redis------------------------
hmset = function(key, ...)
	local input = redis.call("hmset", key, unpack(arg))
end


hsetnx = function(key, ...)
	local input = redis.call("hsetnx", key, unpack(arg))
end


hmget = function(key, ...)
	if next(arg) == nil then return {} end
	local bulk = redis.call( 'HMGET', key, unpack(arg))
	local result = {}
	for i, v in ipairs(bulk) do result[ arg[i] ] = v end
	return result
end


hgetall = function ( key)
  	local bulk = redis.call( 'HGETALL', key)
	local result = {}
	result = toTable(bulk)
	return result
end


zrange = function( key, start, stop, ...)
	local bulk = redis.call( 'ZRANGE', key, start, stop, unpack(arg) )
	if bulk == nil then return {} end
	local result = {}
	result = toTable(bulk)
	
	return result
end


zrangeall = function( key)
	local bulk = redis.call( 'ZRANGE', key, 0, -1, "WITHSCORES" )
	if bulk == nil then return {} end
	local result = {}
	result = toTable(bulk)
	
	return result
end


zadd = function(key, ... )
	redis.call( 'ZADD', key, unpack(arg))
end


del = function(key)
	local bulk = redis.call( 'DEL', key)	
end


hincrby = function(key, field, value)
	local bulk = redis.call("HINCRBY", key, field, value)
end


georadiusbymember= function(aera, key, dist, unitOfLength ,... )
	local bulk = redis.call( "GEORADIUSBYMEMBER", aera, key, dist, unitOfLength, unpack(arg))
	local result ={}
	result = toTable(bulk) 
	return result
end


delAllKeys = function()
	local toDel = redis.call("KEYS", "*consumer*")
	for i, val in pairs(toDel) do
		del(val)
	end
end

---------------------------------------------------------------

getNewValueOfPreferences = function (valueAgent1, valueAgent2, influence)
	local newValue = valueAgent1 + influence * (valueAgent2 - valueAgent1)
	return newValue
end


makeLaptops = function(numberLaptops)
	for i = 1, numberLaptops do
		hmset(tostring(i)..KEY_LAPTOP, 
			"brand", TABLE_BRANDS[math.random(0,3)],
			"budget", math.random(500, 10000),
			"weight", math.random(1, 5)
		)
	end

end


makeAgent =  function(numberOfAgents)
	for i=1, numberOfAgents do
		hmset(tostring(i)..KEY_CONSUMER,
				"id", i,
				"importance_weight", string.format( "%#.2f", math.random()),
				"importance_budget", string.format( "%#.2f", math.random()),
				"importance_brand", string.format( "%#.2f", math.random()),
				"budget", math.random(500, 10000),
				"weight", math.random(1, 5)
			)
		zadd(tostring(i)..KEY_BRAND_PREFERENCES,
				((math.random() * (1 + 1)) - 1), "Lenovo",
				((math.random() * (1 + 1)) - 1), "Dell",
				((math.random() * (1 + 1)) - 1), "Asus"
			)

		for j=1, MAX_NUMBER_OF_FRIENDS do --Na kogo on ma wpływ tak dla ułatwienia
			local newFriend = math.random(1, numberOfAgents)
			if newFriend ~=i then 
				zadd(tostring(i)..KEY_CONSUMER_FRIENDS,
				   math.random(), math.random(1, numberOfAgents)..""
				)
			end
		end
		if math.random()< PERCETAGE_BASE_POPULATION_WHICH_BUY_LAPTOP then
			hmset( KEY_VECTOR_WHO_BY, i, 1)		
		else
			hmset( KEY_VECTOR_WHO_BY, i, 0)	
		end
	end
	
end


updatePreferences = function ( agentToUpdate, agentThatAffects, influence)
	--pobieramy dane o agentach
	local listBrandPreferA1 = zrange( tostring(idAgentToUpdate)..KEY_BRAND_PREFERENCES, 0, -1, "WITHSCORES")
	local listBrandPreferA2 = zrange( tostring(idAgent2)..KEY_BRAND_PREFERENCES, 0, -1, "WITHSCORES")

	--update sortet setów z preferencjami
	for brand, score in pairs(listBrandPreferA1) do
		local newValue = getNewValueOfPreferences( score, listBrandPreferA2[brand], influence)
		zadd(tostring(idAgentToUpdate)..KEY_BRAND_PREFERENCES, newValue, brand)
	end
end


getGradeOfaccordance = function(valAgent, valProduct)
	if valProduct <= valAgent then return 1 end
	if valProduct > valAgent then 
		res = 1-( valProduct - valAgent ) / valAgent
		return res
	end
end


getValueUtilityFunction = function( tabAccordance, tabImportance, size)
	local res = 0
	for key, val in pairs(tabAccordance) do
		res = res + (tonumber(val) * tonumber(tabImportance[key]))
	end
	res = res / size
	return tostring(res)
end


getValueUtilityForProdukt = function( idAgent, idProduct )
	local dataAgent = hgetall(tostring(idAgent)..KEY_CONSUMER)
	local dadaProduct = hgetall(tostring(idProduct)..KEY_LAPTOP)
	local brPrefAgent = zrange(tostring(idAgent)..KEY_BRAND_PREFERENCES, 0, -1, "WITHSCORES")
	local tAccordance = {}	
	local tImportance = {}
	local counter = 0
	for atr, val in pairs(dadaProduct) do
		counter = counter + 1
		if atr == "brand" then 
			tAccordance[atr] = brPrefAgent[val]
			tImportance[atr] = dataAgent["importance_brand"]
		else
			tAccordance[atr] = getGradeOfaccordance(tonumber(dataAgent[atr]), tonumber(val))
			tImportance[atr] = dataAgent["importance_"..atr]
		end
	end	
	local result = getValueUtilityFunction( tAccordance, tImportance, counter)
	return result
end


makeMatrixProductUtilityForAgents = function()
	
	for i=1, MAX_NUMBER_OF_AGENTS do
		for j=1, NUMBER_LAPTOPS do
			hmset(tostring(i)..KEY_MATRIX_UTILITY, j..KEY_LAPTOP, getValueUtilityForProdukt(i,j))
		end
	end
end


maximum = function(tab)
	if tab == {} then return nil end	
	local lMax = -2 --wiemy że funkcja nie przekracza -1 i 1
	for i, j in pairs(tab) do
		if tonumber(j) > tonumber(lMax) then 
			lMax = tonumber(j)
		end
	end
	return lMax
end


updateTableWhoBuy = function()
	for i=1, MAX_NUMBER_OF_AGENTS do
		local agent = hgetall(tostring(i)..KEY_MATRIX_UTILITY)
		local max = maximum(agent)
		if tonumber(max) > TRESHOLD_DECISION_MAKING then
			hmset( KEY_VECTOR_WHO_BY, i, 1)		
		end
	end 
end


getNumberOfDifferents = function(tab1, tab2)
	local counter = 0
	if table.getn(tab1) ~= table.getn(tab2) then return nil end
	for i, j in pairs(tab1) do
		if j ~= tab2[i] then 
			counter = counter + 1
		end
	end
	return counter
end


initialize = function()
	local time_start = redis.call("time")
	makeAgent(MAX_NUMBER_OF_AGENTS)
		
	local startVectorWhoBuy = hgetall(KEY_VECTOR_WHO_BY)
	for i = 1, MAX_NUMBER_OF_AGENTS do
		--update preferencji
		if tonumber(startVectorWhoBuy[i]) == 1 then
			local tabFriends = zrange(tostring(i)..KEY_CONSUMER_FRIENDS, 0, -1, "WITHSCORES")
			for friend, influence in pairs(tabFriends) do
				updatePreferences(friend, i, influence)
			end
		end
	end
	--obliczanie funkcji użyteczności 
	makeMatrixProductUtilityForAgents()
	updateTableWhoBuy()
	local endVectorWhoBuy = hgetall(KEY_VECTOR_WHO_BY)

	local time_end = redis.call("time")
	local result = getNumberOfDifferents( startVectorWhoBuy, endVectorWhoBuy)
	return tostring(result).." "..(time_end[1]+time_end[2]/1000000)-(time_start[1]+time_start[2]/1000000)
end


delAllKeys()
local a = initialize()
return a
	
	