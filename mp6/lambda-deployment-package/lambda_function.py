import json
import sys
import logging
import redis
import pymysql


DB_HOST = "uiuc-mp6-database-1.cluster-ro-ctoz12yvkjdv.us-east-1.rds.amazonaws.com"  
DB_USER = "admin"
DB_PASS = "980102tkuTW!"
DB_NAME = "mp6"
DB_TABLE = "heroes"
REDIS_URL = "redis://mp6-redis-test-001.f84ltx.0001.use1.cache.amazonaws.com:6379"

TTL = 10

class DB:
    def __init__(self, **params):
        params.setdefault("charset", "utf8mb4")
        params.setdefault("cursorclass", pymysql.cursors.DictCursor)

        self.mysql = pymysql.connect(**params)

    def query(self, sql):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def get_idx(self, table_name):
        with self.mysql.cursor() as cursor:
            cursor.execute(f"SELECT MAX(id) as id FROM {table_name}")
            idx = str(cursor.fetchone()['id'] + 1)
            return idx

    def insert(self, idx, data):
        with self.mysql.cursor() as cursor:
            hero = data["hero"]
            power = data["power"]
            name = data["name"]
            xp = data["xp"]
            color = data["color"]
            
            sql = f"INSERT INTO heroes (`id`, `hero`, `power`, `name`, `xp`, `color`) VALUES ('{idx}', '{hero}', '{power}', '{name}', '{xp}', '{color}')"

            cursor.execute(sql)
            self.mysql.commit()

def read(use_cache, indices, Database, Cache):
    result = []
    fromCache = {}
    
    for i in indices:
        if use_cache:
            res = Cache.get(i)
            print(f'res: {res}')
            if res:
                result.append(json.loads(res))
                fromCache[i] = True
        
        if use_cache == False or res == None:
            sql = f'SELECT * FROM heroes WHERE id={i}'
            res = Database.query(sql)
            if res:
                result.append(res[0])
                fromCache[i] = False
                Cache.set(i, json.dumps(res[0]))
        
    print(f'result: {result}')
    print(f'fromCache: {fromCache}')
    return result
    
    
def write(use_cache, sqls, Database, Cache):
    
    for data in sqls:
        idx = Database.get_idx('heroes')
        data['id'] = idx
        Database.insert(idx=idx, data=data)
    
        if use_cache:
            Cache.set(idx, json.dumps(data))

def flush_cache(Cache):
    for key in Cache.scan_iter():
        Cache.delete(key)

def lambda_handler(event, context):
    
    USE_CACHE = (event['USE_CACHE'] == "True")
    REQUEST = event['REQUEST']
    
    # initialize database and cache
    try:
        Database = DB(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME)
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
        sys.exit()
        
    Cache = redis.Redis.from_url(REDIS_URL)
    
    # flush_cache(Cache)  # debug tool to clear cache
    
    result = []
    if REQUEST == "read":
        # event["SQLS"] should be a list of integers
        result = read(USE_CACHE, event["SQLS"], Database, Cache)
    elif REQUEST == "write":
        # event["SQLS"] should be a list of jsons
        write(USE_CACHE, event["SQLS"], Database, Cache)
        result = "write success"
    
    
    return {
        'statusCode': 200,
        'body': result
    }