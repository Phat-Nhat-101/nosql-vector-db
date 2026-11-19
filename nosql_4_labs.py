
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


# 1. MONGODB: thêm đơn, tìm sách, tính doanh thu và tạo index.
def mongo_lab():
    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27017')
    db = client.nosql_lab
    orders = db.orders

    # Mỗi đơn có thể có field khác nhau.
    data = [
        {'_id': 1, 'city': 'Hanoi', 'status': 'paid',
         'items': [{'sku': 'BK-01', 'qty': 2, 'price': 10}]},
        {'_id': 2, 'city': 'Hue', 'status': 'paid', 'note': 'gift',
         'items': [{'sku': 'BK-02', 'qty': 1, 'price': 20}]},
        {'_id': 3, 'city': 'Hanoi', 'status': 'paid', 'coupon': 'NEW',
         'items': [{'sku': 'BK-01', 'qty': 1, 'price': 10}]},
        {'_id': 4, 'city': 'Hue', 'status': 'pending', 'items': []},
        {'_id': 5, 'city': 'Da Nang', 'status': 'shipped', 'tags': ['fast'],
         'items': [{'sku': 'BK-03', 'qty': 2, 'price': 15}]},
    ]
    # upsert giúp chạy lại không bị lỗi trùng _id.
    for order in data:
        orders.replace_one({'_id': order['_id']}, order, upsert=True)

    print('Đơn chứa BK-01:')
    for order in orders.find({'items.sku': 'BK-01'}):
        print(order)

    print('Doanh thu theo thành phố:')
    result = orders.aggregate([
        {'$match': {'_id': {'$in': [1, 2, 3, 4, 5]},
                    'status': {'$in': ['paid', 'shipped']}}},
        {'$unwind': '$items'},
        {'$group': {'_id': '$city', 'revenue': {'$sum': {
            '$multiply': ['$items.qty', '$items.price']}}}},
        {'$sort': {'_id': 1}},
    ])
    print(list(result))  # Da Nang: 30; Hanoi: 30; Hue: 20.

    orders.update_one({'_id': 1}, {'$set': {'status': 'shipped'}})
    orders.update_one({'_id': 3}, {'$unset': {'coupon': ''}})
    orders.replace_one({'_id': 99}, {'_id': 99, 'temporary': True}, upsert=True)
    orders.delete_one({'_id': 99})

    # Benchmark tách riêng để không ảnh hưởng đơn hàng.
    test = db.index_test
    test.drop()
    test.insert_many([{'_id': i, 'city': 'city-' + str(i % 100)}
                      for i in range(10000)])
    query = {'city': 'city-42'}
    for stage in ['Trước index', 'Sau index']:
        if stage == 'Sau index':
            test.create_index('city')
        plan = db.command('explain', {'find': 'index_test', 'filter': query},
                          verbosity='executionStats')
        print(stage, 'docsExamined:', plan['executionStats']['totalDocsExamined'])
        print(plan['queryPlanner']['winningPlan'])
    client.close()


# 2. REDIS: giữ ghế, xác nhận, bán vé, giới hạn lượt và xếp hàng.
def redis_lab():
    import redis
    import uuid
    r = redis.Redis(host='localhost', port=6379, db=15, decode_responses=True)
    # Chỉ xóa key của lab, không dùng FLUSHDB.
    for key in r.scan_iter('lab:*'):
        r.delete(key)

    def race(fn, n=50):
        with ThreadPoolExecutor(max_workers=n) as pool:
            return list(pool.map(fn, range(n)))

    def hold_naive(user):
        if r.get('lab:naive') is None:
            time.sleep(0.01)
            r.set('lab:naive', user, ex=300)
            return True
        return False

    print('Naive winners:', sum(race(hold_naive)))
    print('NX winners:', sum(race(lambda i: bool(r.set('lab:nx', i, nx=True, ex=300)))))

    # Lua chạy nhiều lệnh như một bước nguyên tử.
    HOLD = r.register_script("""
        if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then return 0 end
        if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[3]) then return 1 end
        return 0
    """)
    CONFIRM = r.register_script("""
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        redis.call('SADD', KEYS[2], ARGV[2])
        redis.call('DEL', KEYS[1])
        return 1
    """)
    RELEASE = r.register_script("""
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
    """)

    def hold(event, seat, user):
        return bool(HOLD(keys=[f'lab:hold:{event}:{seat}', f'lab:sold:{event}'],
                         args=[user, seat, 300]))

    def confirm(event, seat, user):
        return bool(CONFIRM(keys=[f'lab:hold:{event}:{seat}', f'lab:sold:{event}'],
                            args=[user, seat]))

    def release(event, seat, user):
        return bool(RELEASE(keys=[f'lab:hold:{event}:{seat}'], args=[user]))

    print(hold('c1', 'A1', 'Linh'))       # True
    print(confirm('c1', 'A1', 'Nam'))     # False
    print(confirm('c1', 'A1', 'Linh'))    # True
    print(hold('c1', 'A1', 'Nam'))        # False: đã bán.
    hold('c1', 'A2', 'Linh')
    print(release('c1', 'A2', 'Linh'))    # True

    BUY = r.register_script("""
        if tonumber(redis.call('GET', KEYS[1]) or '0') <= 0 then return 0 end
        redis.call('DECR', KEYS[1])
        redis.call('SADD', KEYS[2], ARGV[1])
        return 1
    """)
    r.set('lab:stock', 100)
    results = race(lambda i: BUY(keys=['lab:stock', 'lab:buyers'], args=[i]), 500)
    print('Đã bán / còn lại:', sum(results), r.get('lab:stock'))  # 100 / 0

    def allow_fixed(user, limit=5, window=10):
        key = f'lab:fixed:{user}:{int(time.time() // window)}'
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, window, nx=True)
        count, _ = pipe.execute()
        return count <= limit

    SLIDING = r.register_script("""
        local now = tonumber(ARGV[1])
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now-10000)
        if redis.call('ZCARD', KEYS[1]) >= 5 then return 0 end
        redis.call('ZADD', KEYS[1], now, ARGV[2])
        redis.call('PEXPIRE', KEYS[1], 10000)
        return 1
    """)

    def allow_sliding(user):
        return bool(SLIDING(keys=[f'lab:sliding:{user}'],
                            args=[int(time.time()*1000), uuid.uuid4().hex]))

    print('Fixed:', [allow_fixed('Linh') for _ in range(8)])
    print('Sliding:', [allow_sliding('Linh') for _ in range(8)])

    for user in ['u1', 'u2', 'u3']:
        r.zadd('lab:queue', {user: time.time()}, nx=True)
    print('Vị trí u2:', r.zrank('lab:queue', 'u2') + 1)
    print('Cho vào 2 người:', r.zpopmin('lab:queue', 2))

    # 100 khách tranh 20 ghế, mỗi khách mua tối đa một ghế.
    def buyer(i):
        user = str(i)
        for seat in range(20):
            if hold('stress', str(seat), user):
                if confirm('stress', str(seat), user):
                    return seat
        return None

    seats = [s for s in race(buyer, 100) if s is not None]
    print('Số vé / ghế khác nhau:', len(seats), len(set(seats)))  # 20 / 20

    # Bài tập thêm: leaderboard và tìm profile theo thành phố.
    r.zadd('lab:board', {'Linh': 1200, 'Nam': 950, 'Mai': 1430})
    print('Top 3:', r.zrevrange('lab:board', 0, 2, withscores=True))
    r.hset('lab:user:1', mapping={'name': 'Linh', 'city': 'Hanoi'})
    r.sadd('lab:city:Hanoi', '1')
    for uid in r.smembers('lab:city:Hanoi'):
        print(r.hgetall('lab:user:' + uid))
    r.close()


# 3. CASSANDRA: nạp readings và thử ONE / QUORUM / ALL.
def cassandra_lab(action):
    import random
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import WhiteListRoundRobinPolicy, FallthroughRetryPolicy
    from cassandra.query import SimpleStatement

    # Dùng cass1 làm coordinator vì Windows không truy cập IP nội bộ cass2/3.
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=FallthroughRetryPolicy())
    cluster = Cluster(['127.0.0.1'], execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect()
    try:
        session.execute("CREATE KEYSPACE IF NOT EXISTS iot WITH replication = "
                        "{'class':'SimpleStrategy','replication_factor':3}")
        session.execute("""CREATE TABLE IF NOT EXISTS iot.readings_by_sensor (
            sensor_id text, ts timestamp, temperature double, humidity double,
            PRIMARY KEY ((sensor_id),ts)) WITH CLUSTERING ORDER BY (ts DESC)""")
        if action == 'cl':
            for name in ['ONE', 'QUORUM', 'ALL']:
                level = getattr(ConsistencyLevel, name)
                try:
                    session.execute(SimpleStatement("INSERT INTO iot.readings_by_sensor "
                        "(sensor_id,ts,temperature) VALUES ('test',toTimestamp(now()),25)",
                        consistency_level=level))
                    rows = session.execute(SimpleStatement("SELECT count(*) FROM "
                        "iot.readings_by_sensor WHERE sensor_id='test'", consistency_level=level))
                    print(name, 'OK:', rows.one()[0])
                except Exception as error:
                    print(name, type(error).__name__)
        elif action == 'inspect':
            # Xem hash partition key.
            print(list(session.execute('SELECT DISTINCT sensor_id,token(sensor_id) '
                                       'FROM iot.readings_by_sensor LIMIT 5')))
            session.execute("CREATE KEYSPACE IF NOT EXISTS ring1 WITH replication = "
                            "{'class':'SimpleStrategy','replication_factor':1}")
            session.execute('CREATE TABLE IF NOT EXISTS ring1.sensors (sensor_id text PRIMARY KEY)')
            # So sánh query theo partition với ALLOW FILTERING bằng trace.
            for query in ["SELECT * FROM iot.readings_by_sensor WHERE sensor_id='hn-01' LIMIT 3",
                          'SELECT count(*) FROM iot.readings_by_sensor WHERE temperature>31.5 ALLOW FILTERING']:
                result = session.execute(query, trace=True)
                print(list(result))
                for event in result.get_query_trace(max_wait=20).events:
                    print(event.description)
        else:
            insert = session.prepare('INSERT INTO iot.readings_by_sensor '
                '(sensor_id,ts,temperature,humidity) VALUES (?,?,?,?)')
            insert.consistency_level = ConsistencyLevel.ALL
            for sensor in range(1, 31):
                for i in range(100):
                    session.execute(insert, (f'hn-{sensor:02d}',
                        datetime(2026,10,22)+timedelta(minutes=i*5),
                        round(random.uniform(22,32),1), 70.0))
            print('Đã nạp 3000 readings.')
            print(list(session.execute("SELECT * FROM iot.readings_by_sensor "
                                       "WHERE sensor_id='hn-01' LIMIT 3")))
            try:
                session.execute('SELECT * FROM iot.readings_by_sensor WHERE temperature>31.5')
            except Exception as error:
                print('Thiếu partition key:', type(error).__name__)
    finally:
        cluster.shutdown()


# SensorRing — chạy các lệnh terminal sau khi đã nạp dữ liệu:
# python nosql_4_labs.py cassandra inspect
# docker compose -f nosql-compose.yaml exec cass1 nodetool getendpoints ring1 sensors hn-01
# docker compose -f nosql-compose.yaml exec cass1 nodetool ring
#
# Thử consistency khi mất node (đợi nodetool status hiện DN sau mỗi lần stop):
# python nosql_4_labs.py cassandra cl
# docker compose -f nosql-compose.yaml stop cass3
# python nosql_4_labs.py cassandra cl
# docker compose -f nosql-compose.yaml stop cass2
# python nosql_4_labs.py cassandra cl
# RF3: 3 node -> ONE/QUORUM/ALL đều OK; 2 node -> ALL lỗi; 1 node -> chỉ ONE OK.
#
# Thử stale read, từ trạng thái chỉ cass1 đang chạy:
# docker compose -f nosql-compose.yaml start cass2
# Đợi healthy rồi chạy:
# docker compose -f nosql-compose.yaml exec cass1 nodetool disablehandoff
# docker compose -f nosql-compose.yaml exec cass2 nodetool disablehandoff
# docker compose -f nosql-compose.yaml exec cass1 cqlsh
# Trong cqlsh:
# CONSISTENCY QUORUM;
# INSERT INTO iot.readings_by_sensor (sensor_id,ts,temperature) VALUES ('stale-1','2026-11-01',41);
# EXIT;
# docker compose -f nosql-compose.yaml start cass3
# Đợi healthy, rồi:
# docker compose -f nosql-compose.yaml stop cass1 cass2
# docker compose -f nosql-compose.yaml exec cass3 cqlsh
# CONSISTENCY ONE;
# SELECT * FROM iot.readings_by_sensor WHERE sensor_id='stale-1';
# Lần đầu dự kiến 0 rows. Chạy lại dùng id mới để tránh dữ liệu cũ.
# EXIT;
# docker compose -f nosql-compose.yaml start cass1 cass2
# Đợi healthy rồi đọc lại với CONSISTENCY ALL để lấy bản mới và read repair.
# docker compose -f nosql-compose.yaml exec cass3 nodetool repair iot


# 4. NEO4J: giữ nguyên dataset và đủ 7 task CampusGraph.
driver = None

COURSES = [  # (code, title, credits)
    ("CS101", "Intro to Programming", 3), ("MATH101", "Calculus", 3),
    ("MATH102", "Linear Algebra", 3), ("STAT101", "Probability and Statistics", 3),
    ("CS102", "Data Structures", 3), ("DB201", "Databases", 3),
    ("VIZ201", "Data Visualization", 2), ("ML201", "Machine Learning", 4),
    ("DE301", "Data Engineering", 3), ("DB301", "NoSQL Databases", 3),
    ("DL301", "Deep Learning", 4), ("NLP401", "Natural Language Processing", 3),
    ("CAP401", "Capstone Project", 6),
]

REQUIRES = [  # (course, prerequisite)
    ("MATH102", "MATH101"), ("STAT101", "MATH101"), ("CS102", "CS101"),
    ("DB201", "CS102"), ("VIZ201", "CS101"), ("VIZ201", "STAT101"),
    ("ML201", "CS102"), ("ML201", "MATH102"), ("ML201", "STAT101"),
    ("DE301", "DB201"), ("DE301", "CS102"), ("DB301", "DB201"),
    ("DL301", "ML201"), ("NLP401", "DL301"), ("CAP401", "DB301"), ("CAP401", "ML201"),
]

STUDENTS = [  # (id, name, year)
    ("S01", "An", 3), ("S02", "Binh", 2), ("S03", "Chi", 3), ("S04", "Dung", 1),
    ("S05", "Giang", 2), ("S06", "Hoa", 3), ("S07", "Khoa", 2), ("S08", "Lan", 1),
    ("S09", "Minh", 3), ("S10", "Phuong", 2),
]

PASSED = {  # student -> {course: grade}
    "S01": {"CS101": 8.5, "MATH101": 7.0, "CS102": 8.0, "MATH102": 6.5, "STAT101": 7.5, "DB201": 9.0},
    "S02": {"CS101": 7.0, "MATH101": 8.0, "STAT101": 8.5, "VIZ201": 9.0},
    "S03": {"CS101": 9.0, "MATH101": 9.5, "MATH102": 9.0, "STAT101": 8.0, "CS102": 8.5, "ML201": 9.0, "DB201": 7.5},
    "S04": {"CS101": 6.5, "MATH101": 7.5},
    "S05": {"CS101": 8.0, "CS102": 7.5, "DB201": 8.0, "MATH101": 6.0},
    "S06": {"CS101": 9.5, "MATH101": 8.5, "MATH102": 8.0, "STAT101": 9.0, "CS102": 9.0, "DB201": 8.5, "DB301": 9.0, "ML201": 8.0},
    "S07": {"CS101": 7.5, "MATH101": 7.0, "STAT101": 6.5, "CS102": 7.0},
    "S08": {"CS101": 8.0},
    "S09": {"CS101": 8.0, "MATH101": 8.0, "MATH102": 7.5, "STAT101": 7.0, "CS102": 8.0, "DB201": 8.0, "ML201": 7.5, "DL301": 8.5, "VIZ201": 8.0},
    "S10": {"CS101": 9.0, "MATH101": 9.0, "STAT101": 8.0, "VIZ201": 8.5, "CS102": 7.5},
}

FRIENDS = [  # stored once; the friendship means both ways
    ("S01", "S02"), ("S01", "S05"), ("S02", "S04"), ("S04", "S08"), ("S05", "S07"),
    ("S07", "S10"), ("S03", "S06"), ("S03", "S09"), ("S06", "S09"), ("S09", "S10"),
    ("S08", "S10"),
]

def setup():
    """Nạp lại dữ liệu CampusGraph."""
    q = lambda cypher, **params: driver.execute_query(cypher, params)
    q("MATCH (n:CampusLab) DETACH DELETE n")
    q("CREATE CONSTRAINT campus_student_id IF NOT EXISTS FOR (s:CampusStudent) REQUIRE s.id IS UNIQUE")
    q("CREATE CONSTRAINT campus_course_code IF NOT EXISTS FOR (c:CampusCourse) REQUIRE c.code IS UNIQUE")
    q("""UNWIND $rows AS r
         MERGE (c:CampusCourse:CampusLab {code: r[0]}) SET c.title = r[1], c.credits = r[2]""",
      rows=[list(c) for c in COURSES])
    q("""UNWIND $rows AS r
         MATCH (c:CampusCourse {code: r[0]}), (p:CampusCourse {code: r[1]})
         MERGE (c)-[:REQUIRES]->(p)""", rows=[list(r) for r in REQUIRES])
    q("""UNWIND $rows AS r
         MERGE (s:CampusStudent:CampusLab {id: r[0]}) SET s.name = r[1], s.year = r[2]""",
      rows=[list(s) for s in STUDENTS])
    q("""UNWIND $rows AS r
         MATCH (s:CampusStudent {id: r.sid}), (c:CampusCourse {code: r.code})
         MERGE (s)-[p:PASSED]->(c) SET p.grade = r.grade""",
      rows=[{"sid": s, "code": c, "grade": g} for s, d in PASSED.items() for c, g in d.items()])
    q("""UNWIND $rows AS r
         MATCH (a:CampusStudent {id: r[0]}), (b:CampusStudent {id: r[1]})
         MERGE (a)-[:FRIEND]->(b)""", rows=[list(f) for f in FRIENDS])
    counts = driver.execute_query(
        "MATCH (n:CampusLab) RETURN labels(n)[0] AS label, count(*) AS n ORDER BY label").records
    print("loaded:", {r["label"]: r["n"] for r in counts})

def passed_courses(student_id):
    """Task 1. [(code, grade), ...] the student passed, best grade first, then by code."""
    records = driver.execute_query("""
        MATCH (:CampusStudent {id:$sid})-[p:PASSED]->(c:CampusCourse)
        RETURN c.code AS code,p.grade AS grade ORDER BY grade DESC,code
    """, sid=student_id).records
    return [(r["code"],r["grade"]) for r in records]

def prerequisite_chain(code):
    """Task 2. Every direct and indirect prerequisite of a course, as [(code, depth), ...].
    depth = the fewest REQUIRES hops from the course. Sort by depth, then code."""
    records = driver.execute_query("""
        MATCH path=(:CampusCourse {code:$code})-[:REQUIRES*1..]->(p:CampusCourse)
        RETURN p.code AS code,min(length(path)) AS depth ORDER BY depth,code
    """, code=code).records
    return [(r["code"],r["depth"]) for r in records]

def eligible_courses(student_id):
    """Task 3. Codes of courses the student has NOT passed and whose direct
    prerequisites are ALL passed (a course without prerequisites counts). Sorted."""
    records = driver.execute_query("""
        MATCH (s:CampusStudent {id:$sid}),(c:CampusCourse)
        WHERE NOT EXISTS { MATCH (s)-[:PASSED]->(c) }
          AND NOT EXISTS {
            MATCH (c)-[:REQUIRES]->(p:CampusCourse)
            WHERE NOT EXISTS { MATCH (s)-[:PASSED]->(p) }
          }
        RETURN c.code AS code ORDER BY code
    """, sid=student_id).records
    return [r["code"] for r in records]

def suggest_friends(student_id, k=3):
    """Task 4. Students who are not yet friends (either direction), ranked by the number
    of courses both passed. [(name, shared), ...], shared desc then name, top k."""
    records = driver.execute_query("""
        MATCH (s:CampusStudent {id:$sid})-[:PASSED]->(c:CampusCourse)<-[:PASSED]-(o:CampusStudent)
        WHERE s<>o AND NOT EXISTS { MATCH (s)-[:FRIEND]-(o) }
        RETURN o.name AS name,count(DISTINCT c) AS shared
        ORDER BY shared DESC,name LIMIT $k
    """, sid=student_id,k=k).records
    return [(r["name"],r["shared"]) for r in records]

def connection(a_id, b_id):
    """Task 5. Names along the shortest FRIEND path between two students (ignore direction)."""
    if a_id == b_id:
        records = driver.execute_query("MATCH (s:CampusStudent {id:$sid}) RETURN s.name AS name",
                                       sid=a_id).records
        return [r["name"] for r in records]
    records = driver.execute_query("""
        MATCH (a:CampusStudent {id:$a}),(b:CampusStudent {id:$b}),
              p=shortestPath((a)-[:FRIEND*]-(b))
        RETURN [n IN nodes(p) | n.name] AS names
    """, a=a_id,b=b_id).records
    return records[0]["names"] if records else []

def bottleneck_courses(k=3):
    """Task 6. Courses that the most other courses depend on, directly or indirectly.
    [(code, dependents), ...], dependents desc then code, top k."""
    records = driver.execute_query("""
        MATCH (c:CampusCourse)
        OPTIONAL MATCH (d:CampusCourse)-[:REQUIRES*1..]->(c)
        WHERE d<>c
        RETURN c.code AS code,count(DISTINCT d) AS dependents
        ORDER BY dependents DESC,code LIMIT $k
    """, k=k).records
    return [(r["code"],r["dependents"]) for r in records]

def _record_pass_tx(tx, student_id, code, grade):
    # Task 7: use tx.run(...) for every query here, so they share ONE transaction.
    row = tx.run("""
        OPTIONAL MATCH (s:CampusStudent {id:$sid})
        OPTIONAL MATCH (c:CampusCourse {code:$code})
        RETURN s IS NOT NULL AND c IS NOT NULL AS ok
    """, sid=student_id,code=code).single()
    if not row["ok"]:
        raise ValueError(f"unknown student or course: {student_id}, {code}")
    missing = tx.run("""
        MATCH (s:CampusStudent {id:$sid}),(c:CampusCourse {code:$code})-[:REQUIRES]->(p)
        WHERE NOT EXISTS { MATCH (s)-[:PASSED]->(p) }
        RETURN p.code AS code ORDER BY code
    """, sid=student_id,code=code).value()
    if missing:
        raise ValueError(f"missing prerequisites: {missing}")
    tx.run("""
        MATCH (s:CampusStudent {id:$sid}),(c:CampusCourse {code:$code})
        MERGE (s)-[p:PASSED]->(c) SET p.grade=$grade
    """, sid=student_id,code=code,grade=grade).consume()

def record_pass(student_id, code, grade):
    """Task 7. Record that a student passed a course, inside ONE write transaction.
    Raise ValueError (and write nothing) if the student or course does not exist or a
    direct prerequisite is missing. Recording the same pass twice must not duplicate it."""
    with driver.session() as session:
        session.execute_write(_record_pass_tx, student_id, code, grade)

def check():
    setup()

    def expect_error(fn):
        try:
            fn()
        except ValueError as e:
            return str(e)
        return None

    def after_record():
        record_pass("S08", "CS102", 7.0)
        record_pass("S08", "CS102", 7.5)          # same pass again: update, not duplicate
        n = driver.execute_query("""
            MATCH (:CampusStudent {id: 'S08'})-[p:PASSED]->(:CampusCourse {code: 'CS102'})
            RETURN count(p) AS n, max(p.grade) AS g""").records[0]
        return (n["n"], n["g"], eligible_courses("S08"))

    tests = [
        ("1 passed_courses", lambda: passed_courses("S04"), [("MATH101", 7.5), ("CS101", 6.5)]),
        ("2 prerequisite_chain", lambda: prerequisite_chain("DE301"),
         [("CS102", 1), ("DB201", 1), ("CS101", 2)]),
        ("3 eligible_courses", lambda: eligible_courses("S01"), ["DB301", "DE301", "ML201", "VIZ201"]),
        ("4 suggest_friends", lambda: suggest_friends("S02"),
         [("Minh", 4), ("Phuong", 4), ("Chi", 3)]),
        ("5 connection", lambda: connection("S01", "S08"), ["An", "Binh", "Dung", "Lan"]),
        ("6 bottleneck_courses", lambda: bottleneck_courses(),
         [("CS101", 9), ("CS102", 7), ("MATH101", 7)]),
        ("7a record_pass refuses", lambda: expect_error(lambda: record_pass("S04", "ML201", 8.0)),
         "missing prerequisites: ['CS102', 'MATH102', 'STAT101']"),
        ("7b record_pass writes", after_record, (1, 7.5, ["DB201", "MATH101"])),
    ]
    failed = 0
    for name, fn, expected in tests:
        try:
            got = fn()
        except NotImplementedError:
            print(f"TODO  {name}")
            continue
        except Exception as e:
            failed += 1
            print(f"ERROR {name}: {type(e).__name__}: {e}")
            continue
        status = "PASS " if got == expected else "FAIL "
        print(f"{status} {name}")
        if got != expected:
            failed += 1
            print(f"        expected {expected}\n        got      {got}")


    if failed:
        raise AssertionError(f"{failed} CampusGraph checks failed")


def neo4j_lab():
    from neo4j import GraphDatabase
    global driver
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j','learnneo4j'))
    try:
        check()  # Nạp dữ liệu và chạy 8 check trong đề.
    finally:
        driver.close()


# Cấu hình Docker cho 4 lab; không cần tự gõ YAML.
COMPOSE = '''# Docker Compose — cả 4 lab. Cassandra cần khởi động tuần tự.
x-cassandra: &cassandra
  image: cassandra:5
  environment:
    CASSANDRA_CLUSTER_NAME: sensorring
    CASSANDRA_SEEDS: cass1
    MAX_HEAP_SIZE: 512M
    HEAP_NEWSIZE: 128M
  healthcheck:
    test: ["CMD", "cqlsh", "-e", "describe keyspaces"]
    interval: 15s
    timeout: 10s
    retries: 20
    start_period: 60s
services:
  mongo:
    image: mongo:8
    ports: ["127.0.0.1:27017:27017"]
    volumes: [mongo-data:/data/db]
  redis:
    image: redis:8
    ports: ["127.0.0.1:6379:6379"]
  neo4j:
    image: neo4j:5
    ports: ["127.0.0.1:7474:7474", "127.0.0.1:7687:7687"]
    environment:
      NEO4J_AUTH: neo4j/learnneo4j
    volumes: [neo4j-data:/data]
  cass1:
    <<: *cassandra
    ports: ["127.0.0.1:9042:9042"]
  cass2:
    <<: *cassandra
    depends_on:
      cass1: {condition: service_healthy}
  cass3:
    <<: *cassandra
    depends_on:
      cass2: {condition: service_healthy}
volumes:
  mongo-data:
  neo4j-data:
'''


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    lab = sys.argv[1] if len(sys.argv)>1 else 'help'
    if lab == 'init':
        path = Path(__file__).with_name('nosql-compose.yaml')
        if not path.exists():
            path.write_text(COMPOSE, encoding='utf-8')
        print('Docker config:', path)
    elif lab == 'mongo':
        mongo_lab()
    elif lab == 'redis':
        redis_lab()
    elif lab == 'cassandra':
        action = sys.argv[2] if len(sys.argv)>2 else 'load'
        cassandra_lab(action)
    elif lab == 'neo4j':
        neo4j_lab()
    else:
        print(__doc__)
