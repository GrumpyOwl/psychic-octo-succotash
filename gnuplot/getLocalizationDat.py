import lupa as lupa
import redis
import time

import geodis


def diagram():
    r = redis.StrictRedis(host="127.0.0.1", port = 6379, db=0,  decode_responses=True)
    lua = """
        local locations = redis.call('GEORADIUS', KEYS[1], '52', '19', '1000', 'km', 'WITHCOORD')
        return locations
        """
    #result = r.register_script("return redis.call('GEORADIUS', 'Polska', '52', '19', '1000', 'km', 'WITHCOORD')", 0)  local locations = redis.call('GEORADIUS', 'Polska', '52', '19', '1000', 'km', 'WITHCOORD')
    result = r.register_script(lua)

    return result(keys = ['Polska'])

lista = diagram()
plik = open('localization.dat', 'w')
for i, v in lista:
    plik.write(str(i)+"\t"+v[0]+"\t"+v[1]+"\n")

plik.close()
