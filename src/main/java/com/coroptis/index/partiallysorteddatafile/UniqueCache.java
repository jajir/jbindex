package com.coroptis.index.partiallysorteddatafile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.coroptis.index.Pair;
import com.coroptis.index.PairFileReader;
import com.coroptis.index.basic.ValueMerger;

public class UniqueCache<K, V> {

    private final TreeMap<K, V> map;
    private final ValueMerger<K, V> merger;

    public UniqueCache(final ValueMerger<K, V> merger, final Comparator<K> keyComparator) {
        this.merger = Objects.requireNonNull(merger);
        this.map = new TreeMap<>(keyComparator);
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
    
    
    public PairFileReader<K, V> openReader() {
	return new UniqueCacheReader<>(getStream().iterator());
    }
    
    public PairFileReader<K, V> openClonedReader() {
	final List<Pair<K,V>> out = new ArrayList<>();
	getStream().forEach(out::add);
	return new UniqueCacheReader<>(out.iterator());
    }

    /**
     * Get unsorted stream of key value pairs
     * 
     * @return unsorted stream of key value pairs
     */
    public Stream<Pair<K, V>> getStream() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()));
    }

}
