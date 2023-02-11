package com.coroptis.index.partiallysorteddatafile;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.sorteddatafile.PairComparator;

public class UniqueCache<K, V> {

    private final Comparator<K> keyComparator;
    private final TreeMap<K, V> map;
    private final ValueMerger<K, V> merger;

    public UniqueCache(final ValueMerger<K, V> merger,
            final Comparator<K> keyComparator) {
        this.keyComparator = Objects.requireNonNull(keyComparator);
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

    /**
     * It's unsorted.
     * 
     * @return
     */
    public List<Pair<K, V>> toList() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * It's unsorted.
     * 
     * @return
     */
    public PairReader<K, V> openReader() {
        return new UniqueCacheReader<>(getStream().iterator());
    }

    public List<Pair<K, V>> getAsSortedList(){
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()))
                .sorted(new PairComparator<>(keyComparator))
                .collect(Collectors.toList());        
    }
    
    /**
     * It's unsorted.
     * 
     * @return
     */
    public PairReader<K, V> openSortedClonedReader() {
        return new UniqueCacheReader<>(getAsSortedList().iterator());
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
