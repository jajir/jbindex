package com.coroptis.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.coroptis.index.fileindex.Pair;

public class UniqueCache<K, V> {

    private final Map<K, V> map = new HashMap<>();
    private final Merger<K, V> merger;

    UniqueCache(final Merger<K, V> merger) {
	this.merger = Objects.requireNonNull(merger);
    }

    void add(final Pair<K, V> pair) {
	map.merge(pair.getKey(), pair.getValue(),
		(oldVal, newVal) -> merger.merge(pair.getKey(), oldVal, newVal));
    }

    void clear() {
	map.clear();
    }

    List<Pair<K, V>> toList() {
	return map.entrySet().stream()
		.map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()))
		.collect(Collectors.toList());
    }

}
