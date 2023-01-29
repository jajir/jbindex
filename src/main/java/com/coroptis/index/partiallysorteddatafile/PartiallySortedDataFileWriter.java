package com.coroptis.index.partiallysorteddatafile;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.SortSupport;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;

public class PartiallySortedDataFileWriter<K, V> implements CloseableResource {

    private final int howManySortInMemory;
    private final BasicIndex<K, V> basicIndex;
    private final Comparator<? super K> keyComparator;
    private final SortSupport<K, V> sortSupport;

    private int cx = 0;
    private int fileCounter = 0;
    private final UniqueCache<K, V> cache;

    public PartiallySortedDataFileWriter(final String fileName, final ValueMerger<K, V> merger,
            final int howManySortInMemory, final BasicIndex<K, V> basicIndex,
            final Comparator<? super K> keyComparator) {
        Objects.requireNonNull(fileName);
        this.howManySortInMemory = Objects.requireNonNull(howManySortInMemory);
        this.basicIndex = Objects.requireNonNull(basicIndex);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.sortSupport = new SortSupport<K, V>(basicIndex, merger, fileName);
        this.cache = new UniqueCache<>(merger);
    }

    public void put(final Pair<K, V> pair) {
        if (fileCounter < 0) {
            throw new IllegalStateException("Attempt to put values into closed index.");
        }
        cache.add(pair);
        cx++;
        if (cx % howManySortInMemory == 0) {
            writeCacheToFile(fileCounter);
            fileCounter++;
        }
    }

    private void writeCacheToFile(final int fileCounter) {
        if (cache.isEmpty()) {
            return;
        }
        final List<Pair<K, V>> tempCache = cache.toList();
        cache.clear();
        Collections.sort(tempCache,
                (pair1, pair2) -> keyComparator.compare(pair1.getKey(), pair2.getKey()));
        final String fileName = sortSupport.makeFileName(0, fileCounter);
        final SortedDataFile<K, V> sortedFile = basicIndex.getSortedDataFile(fileName);
        try (final SortedDataFileWriter<K, V> writer = sortedFile.openWriter()) {
            tempCache.forEach(pair -> writer.put(pair));
        }
    }

    @Override
    public void close() {
        writeCacheToFile(fileCounter);
        fileCounter = -1;
    }
}
