import time
import pickle
import msgpack
import json as json_module
from pyrkyv import archive, load_archived

data = {"name": "Alice", "age": 30, "city": "New York"}
archived = archive(data)
pickled = pickle.dumps(data)
packed = msgpack.packb(data)
json = json_module.dumps(data)
rkyv_value = load_archived(archived)
pickle_value = pickle.loads(pickled)
msgpack_value = msgpack.unpackb(packed)
json_value = json_module.loads(json)

print("\n=== LOAD BENCHMARK ===")





N = 100_000

# rkyv
start = time.time()
for _ in range(N):
    load_archived(archived)
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

""" 
=== LOAD BENCHMARK ===
pyrkyv load: 1.6823744773864746
pickle load: 0.17903757095336914
msgpack load: 0.15428996086120605
json load: 0.47916150093078613

"""


print("=== LAZY LOOKUP BENCHMARK ===")

start = time.time()
for _ in range(N):
    rkyv_value["name"]
print("rkyv:", time.time() - start)

start = time.time()
for _ in range(N):
    json_value["name"]
print("json:", time.time() - start)


start = time.time()
for _ in range(N):
    pickle_value["name"]
print("pickle:", time.time() - start)



start = time.time()
for _ in range(N):
    msgpack_value["name"]
print("msgpack:", time.time() - start)


""" 
=== LAZY LOOKUP BENCHMARK ===
pyrkyv: 0.31873655319213867
json: 0.014999866485595703
pickle: 0.014992237091064453
msgpack: 0.015999794006347656


"""

