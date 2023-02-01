package com.coroptis.index.fastindex;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairFileReader;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.partiallysorteddatafile.UniqueCache;
import com.coroptis.index.simpledatafile.MergedPairReader;
import com.coroptis.index.simpledatafile.SimpleDataFile;
import com.coroptis.index.type.TypeDescriptor;

public class FastIndex<K, V> implements CloseableResource {

    private final long maxNumberOfKeysInCache;
    private final long maxNumeberOfKeysInSegmentCache;
    private final long maxNumeberOfKeysInSegment;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ValueMerger<K, V> valueMerger;
    private final FastIndexFile<K> fastIndexFile;
    private final UniqueCache<K, V> cache;

    public static <M, N> FastIndexBuilder<M, N> builder() {
	return new FastIndexBuilder<M, N>();
    }

    public FastIndex(final Directory directory, final TypeDescriptor<K> keyTypeDescriptor,
	    final TypeDescriptor<V> valueTypeDescriptor, final ValueMerger<K, V> valueMerger,
	    final long maxNumberOfKeysInCache, final long maxNumeberOfKeysInSegmentCache,
	    final long maxNumeberOfKeysInSegment) {
	this.maxNumberOfKeysInCache = Objects.requireNonNull(maxNumberOfKeysInCache);
	this.maxNumeberOfKeysInSegmentCache = Objects.requireNonNull(maxNumeberOfKeysInSegmentCache);
	this.maxNumeberOfKeysInSegment = Objects.requireNonNull(maxNumeberOfKeysInSegment);
	this.directory = Objects.requireNonNull(directory);
	this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
	this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
	this.valueMerger = Objects.requireNonNull(valueMerger);
	this.fastIndexFile = new FastIndexFile<K>(directory, keyTypeDescriptor);
	this.cache = new UniqueCache<>(valueMerger);
    }

    public void put(final Pair<K, V> pair) {
	Objects.requireNonNull(pair);
	cache.add(pair);
	if (cache.size() > maxNumberOfKeysInCache) {
	    compact();
	}
    }

    private void compact() {
	// It's stupid more than one pair falls into opened segment.
	CompactSupport<K, V> support = new CompactSupport<>(this, fastIndexFile);
	cache.getStream().forEach(pair -> {
	    support.compact(pair);
	});
	support.compactRest();
	cache.clear();
	optionallyCompactSegments();
    }

    SimpleDataFile<K, V> getSegment(final int pageId) {
	final SimpleDataFile<K, V> sdf = new SimpleDataFile<>(directory, getFileName(pageId), keyTypeDescriptor,
		valueTypeDescriptor, valueMerger);
	return sdf;
    }

    /**
     * Verify that number of keys in segments doesn't exceed some threshold. When it
     * exceed than segment is merged or split into two smaller segment.
     */
    private void optionallyCompactSegments() {
	/*
	 * Defensive copy have to be done, because further splitting will affect list
	 * size. In the future it will be slow.
	 */
	final List<Pair<K, Integer>> list = fastIndexFile.getPagesAsStream().collect(Collectors.toList());
	list.forEach(pair -> {
	    final SimpleDataFile<K, V> sdf = new SimpleDataFile<>(directory, getFileName(pair.getValue()),
		    keyTypeDescriptor, valueTypeDescriptor, valueMerger);
	    if (sdf.getStats().getNumberOfPairsInMainFile() > maxNumeberOfKeysInSegment) {
		final int newSegmentId = (int) (fastIndexFile.getPagesAsStream().count());
		final K newPageKey = sdf.split(getFileName(newSegmentId));
		fastIndexFile.insertPage(newPageKey, newSegmentId);
	    }
	    if (sdf.getStats().getNumberOfPairsInCache() > maxNumeberOfKeysInSegmentCache) {
		sdf.merge();
	    }
	});
    }

    /**
     * It allows to iterate over all stored data in sorted way.
     * 
     * @return
     */
    public PairFileReader<K, V> openReader() {
	final FastIndexReader<K, V> fastIndexreader = new FastIndexReader<>(this, fastIndexFile);
	final PairFileReader<K, V> cacheReader = cache.openClonedReader();
	final MergedPairReader<K, V> mergedPairReader = new MergedPairReader<>(cacheReader, fastIndexreader,
		valueMerger, keyTypeDescriptor.getComparator());
	return mergedPairReader;
    }

    private String getFileName(final int fileId) {
	String name = String.valueOf(fileId);
	while (name.length() < 5) {
	    name = "0" + name;
	}
	return "segment-" + name;
    }

    @Override
    public void close() {
	compact();
	fastIndexFile.close();
    }

}
