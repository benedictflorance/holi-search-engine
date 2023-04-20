package cis5550.kvs;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache<K, V> {
	
	Deque<K> lru;
	Map<K, V> cache;
	int capacity;
	int size;
	
	public LRUCache(int capacity) {
		this.capacity = capacity;
		size = 0;
		lru = new LinkedList<K>();
		cache = new ConcurrentHashMap<K, V>();
	}
	
	public V get(K key) {
		if (!cache.containsKey(key)) {
			return null;
		}
		lru.remove(key);
		lru.addLast(key);
		return cache.get(key);
	}
	
	public V put(K key, V val) {
		V evicted = null;
		if (size > capacity) {
			evicted = evict();
		}
		lru.addLast(key);
		cache.put(key, val);
		size++;
		return evicted;
	}
	
	public void silentPut(K key, V val) {
		cache.put(key, val);
	}
	public boolean exists(K key) {
		return cache.containsKey(key);
	}
	
	public V evict() {
		size--;
		K key = lru.removeFirst();
		V val = cache.remove(key);
		return val;
	}
	

}
