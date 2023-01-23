package com.coroptis.index.partiallysorteddatafile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.sorteddatafile.Pair;

class UniqueCache<K, V> {

    private final Map<K, V> map = new HashMap<>();
    private final ValueMerger<K, V> merger;

    UniqueCache(final ValueMerger<K, V> merger) {
        this.merger = Objects.requireNonNull(merger);
    }

    void add(final Pair<K, V> pair) {
        map.merge(pair.getKey(), pair.getValue(),
                (oldVal, newVal) -> merger.merge(pair.getKey(), oldVal, newVal));
    }

    void clear() {
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    List<Pair<K, V>> toList() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

}
