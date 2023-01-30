package com.coroptis.index.partiallysorteddatafile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.coroptis.index.Pair;
import com.coroptis.index.basic.ValueMerger;

public class UniqueCache<K, V> {

    private final Map<K, V> map = new HashMap<>();
    private final ValueMerger<K, V> merger;

    public UniqueCache(final ValueMerger<K, V> merger) {
        this.merger = Objects.requireNonNull(merger);
    }

    public void add(final Pair<K, V> pair) {
        map.merge(pair.getKey(), pair.getValue(), (oldVal, newVal) -> merger
                .merge(pair.getKey(), oldVal, newVal));
    }

    public void clear() {
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public List<Pair<K, V>> toList() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public Stream<Pair<K, V>> getStream() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()));
    }

}
