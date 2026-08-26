import bisect
import hashlib


class HashRing:
    """
    Consistent hash ring with virtual nodes.

    Built once at startup from a static, known list of physical nodes.
    No support for adding/removing nodes at runtime (out of scope) —
    but the *ring lookup* is exactly what we reuse in milestone 4 to
    find failover targets, since "next node clockwise" is a ring concept.
    """

    def __init__(self, nodes: list[str], virtual_nodes: int = 150):
        """
        nodes: list of physical node identifiers, e.g. ["node-1", "node-2", "node-3"]
        virtual_nodes: number of points each physical node gets on the ring
        """
        self.virtual_nodes = virtual_nodes
        self.ring: dict[int, str] = {}       # hash -> physical node
        self.sorted_hashes: list[int] = []   # sorted ring positions for binary search

        for node in nodes:
            self._add_node(node)

    def _hash(self, key: str) -> int:
        # md5 is fine here — we're not doing anything cryptographic,
        # just need a well-distributed integer from a string.
        digest = hashlib.md5(key.encode("utf-8")).hexdigest()
        return int(digest, 16)

    def _add_node(self, node: str) -> None:
        for i in range(self.virtual_nodes):
            vnode_key = f"{node}#{i}"
            h = self._hash(vnode_key)
            self.ring[h] = node
            bisect.insort(self.sorted_hashes, h)

    def get_node(self, key: str) -> str:
        """Return the physical node that owns this key."""
        if not self.ring:
            raise ValueError("Hash ring is empty")

        h = self._hash(key)
        # Find the first ring position >= h (clockwise search).
        idx = bisect.bisect(self.sorted_hashes, h)

        # Wrap around: if h is past the last ring position, it belongs
        # to the first node on the ring (the ring is circular).
        if idx == len(self.sorted_hashes):
            idx = 0

        return self.ring[self.sorted_hashes[idx]]

    def get_preference_list(self, key: str, count: int) -> list[str]:
        """
        Return `count` distinct physical nodes for this key, in ring order:
        [primary, replica_1, replica_2, ...].

        This is the list milestone 3 (replication) and milestone 4
        (failover) both build on — "primary" is just index 0, and a
        "replica" is just the next distinct physical node clockwise.
        """
        if not self.ring:
            raise ValueError("Hash ring is empty")

        h = self._hash(key)
        idx = bisect.bisect(self.sorted_hashes, h)

        result = []
        seen_physical_nodes = set()
        n = len(self.sorted_hashes)

        # Walk clockwise around the ring, collecting distinct physical
        # nodes until we have enough. We might pass the same physical
        # node's vnode multiple times, so dedupe with seen_physical_nodes.
        steps = 0
        while len(result) < count and steps < n:
            pos = (idx + steps) % n
            physical_node = self.ring[self.sorted_hashes[pos]]
            if physical_node not in seen_physical_nodes:
                result.append(physical_node)
                seen_physical_nodes.add(physical_node)
            steps += 1

        return result