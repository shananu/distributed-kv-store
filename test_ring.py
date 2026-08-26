# test_ring.py — not part of the app, just for you to inspect distribution
from app.hash_ring import HashRing

ring = HashRing(nodes=["node-1", "node-2", "node-3"], virtual_nodes=150)

# Check distribution across 10,000 sample keys
from collections import Counter
counts = Counter(ring.get_node(f"key-{i}") for i in range(10_000))
print(counts)

# Check preference lists (primary + replicas)
print(ring.get_preference_list("foo", count=3))