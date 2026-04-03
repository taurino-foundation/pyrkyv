import time
import pickle
import msgpack
import json as json_module
from pyrkyv import archive, access_archived

data = {"name": "Alice", "age": 30, "city": "New York"}
archived = archive(data)
pickled = pickle.dumps(data)
packed = msgpack.packb(data)
json = json_module.dumps(data)
rkyv_value = access_archived(archived)
pickle_value = pickle.loads(pickled)
msgpack_value = msgpack.unpackb(packed)
json_value = json_module.loads(json)

print("\n=== LOAD BENCHMARK ===")



# ======================
# LOAD BENCHMARK
# ======================

N = 100_000

# rkyv
start = time.time()
for _ in range(N):
    access_archived(archived)
print("rkyv load:", time.time() - start)

# pickle
start = time.time()
for _ in range(N):
    pickle.loads(pickled)
print("pickle load:", time.time() - start)

# msgpack
start = time.time()
for _ in range(N):
    msgpack.unpackb(packed)
print("msgpack load:", time.time() - start)

# json
start = time.time()
for _ in range(N):
    json_module.loads(json)
print("json load:", time.time() - start)




print("=== LAZY LOOKUP BENCHMARK ===")

start = time.time()
for _ in range(100_000):
    rkyv_value["name"]
print("rkyv:", time.time() - start)

start = time.time()
for _ in range(100_000):
    json_value["name"]
print("json:", time.time() - start)


start = time.time()
for _ in range(100_000):
    pickle_value["name"]
print("pickle:", time.time() - start)



start = time.time()
for _ in range(100_000):
    msgpack_value["name"]
print("msgpack:", time.time() - start)

