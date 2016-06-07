local mt = setmetatable(_G, nil) --moduł no Globals
redis.replicate_commands()
  
MAX_NUMBER_OF_AGENTS = 100000
MAX_NUMBER_OF_FRIENDS = 20 --liczba friendsów
NUMBER_NEIGHBOUR = 20 --ilość sąsiadów
NUMBER_OF_INTERVAL = 3


KEY_VACTOR_WHO_BY = "consumer:who_buy"
--":"<- musi być bo pierwszy jest index
KEY_CONSUMER = ":consumer"
KEY_LAPTOP = ":laptop"
KEY_COUNTER_BRAND = "counter:brand:"
KEY_BRAND_PREFERENCES = ":consumer:brand_preferences"
KEY_CONSUMER_FRIENDS = ":consumer:friends"
KEY_AVERAGE_INTEREST_BRAND = "average:brand"


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
	if next(arg) == nil then return nil end
	local input = redis.call("hmset", key, unpack(arg))
end


hsetnx = function(key, ...)
	if next(arg) == nil then return nil end
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
	local bulk = redis.call( 'ZRANGE', key, 0, -1, unpack(arg) )
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
	return "Removed."
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
-------------------------------------------------


getNewValueOfPreferences = function (valueAgent1, valueAgent2, influence)
	local newValue = valueAgent1 + influence * (valueAgent2 - valueAgent1)
	return newValue
end


updatePreferences = function ( idAgent1, idAgent2)
	
	--pobieramy dane o agentach
	local listFriendAgent1 = zrange( tostring(idAgent1)..KEY_CONSUMER_FRIENDS, 0, -1, "WITHSCORES")
	local listBrandPreferencesAgent1 = zrange( tostring(idAgent1)..KEY_BRAND_PREFERENCES, 0, -1, "WITHSCORES")
	local listBrandPreferencesAgent2 = zrange( tostring(idAgent2)..KEY_BRAND_PREFERENCES, 0, -1, "WITHSCORES")
	local influence = listFriendAgent1[idAgent2..""]

	--update sortet setów z preferencjami
	local newValue = getNewValueOfPreferences( score, listBrandPreferencesAgent2[brand], influence)
	for brand, score in pairs(listBrandPreferencesAgent1) do
		zadd( tostring(idAgent1)..KEY_BRAND_PREFERENCES, newValue, brand)
	end

end


toContain = function(elem, tab)
	if tab == nil then return 0 end
	for i, v in pairs(tab) do
		if (i == tonumber(elem)) or (i == tostring(elem)) then
			return 1
		end
	end
	return 0	
end


randFromTable = function( table, sizeTable)
	
	local counter = math.random( 1, sizeTable) 
	for key, val in pairs( table) do
		counter = counter - 1 
		if counter == 0 then 
			return key
		end
	end	
end

 
makeUpdatePreferences = function( numberOfAgentsToUpdate, maxNumberOfAgents)
	
	for i = 1, numberOfAgentsToUpdate do 
		local idAgent = math.random(1, maxNumberOfAgents)
		local listFriends = zrange(tostring(idAgent)..KEY_CONSUMER_FRIENDS, 0, -1, "WITHSCORES")
		
		for j = 1, numberOfAgentsToUpdate do
			local idFriend = tonumber(randFromTable(listFriends, MAX_NUMBER_OF_FRIENDS))			
			updatePreferences(idAgent, idFriend)
		end
	end	
end


meetFriend = function( idAgent1, idAgent2)
	zadd( tostring(idAgent1)..KEY_CONSUMER_FRIENDS,  math.random(), idAgent2.."")
	zadd( tostring(idAgent2)..KEY_CONSUMER_FRIENDS,  math.random(), idAgent1.."")
end


meetNeighbours = function( idAgent, dist, numberOfNeighbours)
	local listNeighbours = georadiusbymember( "Polska", idAgent, dist, "km", "WITHDIST", "count", (numberOfNeighbours+1).."")--funkcja.....
	local listFriends = zrange( tostring(idAgent)..KEY_CONSUMER_FRIENDS, 0, -1, "WITHSCORES")
	
	for i, v in  pairs( listNeighbours) do
		if  v[1] ~=  tostring( idAgent) then --jeśli to nie on sam…
			meetFriend( idAgent, v[1])
		end
	end
	
	listFriends = zrange( tostring(idAgent)..KEY_CONSUMER_FRIENDS, 0, -1, "WITHSCORES")
	local counter = 0
	del( tostring(idAgent)..KEY_CONSUMER_FRIENDS)
	
	for key, value in pairs(listFriends) do
		counter = counter + 1  
		zadd( tostring(idAgent)..KEY_CONSUMER_FRIENDS, value, key)
		if counter == 20 then break end
	end	
end


everyoneMeetNeighbours = function()
	
	for i=1 , MAX_NUMBER_OF_AGENTS do 
		meetNeighbours(i, 20, NUMBER_NEIGHBOUR)
	end
end


calculateAverageInterestBrand = function()
	local tSum = {}
	
	--Sumowanie wartosci 
	for i=1, MAX_NUMBER_OF_AGENTS do 
		brandScores = zrange(tostring(i)..KEY_BRAND_PREFERENCES, 0, -1, "WITHSCORES")
		for brand, score in pairs(brandScores) do
			if tSum[brand] == nil then 
				tSum[brand] = score
			else
				tSum[brand] = tSum[brand] + tonumber(score)
			end
		end
	end
	
	--Obliczanie średniej
	for brand, score in pairs(tSum) do
		tSum[brand] = score/MAX_NUMBER_OF_AGENTS
	end 
	
	--Przypisywanie średnich do zmiennej
	for brand, score in pairs(tSum) do
		hmset(KEY_AVERAGE_INTEREST_BRAND, brand, score)
	end 
end


getDataToHistogram = function()
	
	local tCounter = {}
	
	for i=1, MAX_NUMBER_OF_AGENTS do
		brandScores = zrange(i..KEY_BRAND_PREFERENCES, 0, -1, "WITHSCORES")
		
		for brand, score in pairs(brandScores) do
			number_range = math.floor(score * NUMBER_OF_INTERVAL)
			
			if tCounter[brand] == nil then tCounter[brand] = {} end
			
			if tCounter[brand][number_range] == nil then 
				tCounter[brand][number_range] = 1
			else
				tCounter[brand][number_range] = tCounter[brand][number_range] + 1
			end
		end
	end
	
	for brand, interval in pairs(tCounter) do
		for number, counter in pairs(interval) do 
			hmset(KEY_COUNTER_BRAND..brand, number, counter)
		end
	
	end
end	
	
	
getDataToHistogram()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	