package com.coroptis.index.jbindex;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.Pair;

public class MergeIndexAndConsumer<K, V> implements CloseableResource {

    private final Directory inputDirectory;
    private final IndexReader<K, V> inputIndexReader;
    private final IndexWriter<K, V> finalIndexWriter;
    private final ValueMerger<K, V> merger;
    final Comparator<? super K> keyComparator;

    public MergeIndexAndConsumer(final Directory inputIndex, final Directory output,
            final ValueMerger<K, V> merger, final Class<?> keyClass, final Class<?> valueClass,
            final int blockSize) {
        this.inputDirectory = Objects.requireNonNull(inputIndex);
        this.merger = Objects.requireNonNull(merger);
        this.keyComparator = null;
        final BasicIndex<K, V> basicIndexInput = new BasicIndex<>(inputIndex, null, null, null);
        final IndexIterator<K, V> inputIterator = new IndexSearcher<K, V>(inputDirectory, keyClass,
                valueClass, basicIndexInput).getIterator();
        inputIndexReader = new IndexReader<>(inputIterator);
        finalIndexWriter = new IndexWriter<>(output, blockSize, keyClass, valueClass);
    }

    public Consumer<Pair<K, V>> getConsumer() {
        return pair -> mergePair(pair);
    }

    /**
     * This is called when produces of pairs to merge produce one pair
     * 
     * @param pair1 required pair
     */
    private void mergePair(final Pair<K, V> pair1) {
        while (!mergeOne(pair1))
            ;
    }

    /**
     * 
     * @param pair1 pair called from exposed consumer
     * @return return <code>true</code> when given pair was consumed.
     */
    private boolean mergeOne(final Pair<K, V> pair1) {
        Objects.requireNonNull(pair1);
        if (inputIndexReader.readCurrent().isPresent()) {
            final Pair<K, V> pair2 = inputIndexReader.readCurrent().get();
            final int cmp = keyComparator.compare(pair1.getKey(), pair2.getKey());
            if (cmp == 0) {
                // p1 == p2
                final Pair<K, V> out = merger.merge(pair1, pair2);
                finalIndexWriter.put(out.getKey(), out.getValue());
                inputIndexReader.moveToNext();
                return true;
            } else if (cmp < 0) {
                // p1 < p2
                finalIndexWriter.put(pair1.getKey(), pair1.getValue());
                return true;
            } else {
                // p1 > p2
                finalIndexWriter.put(pair2.getKey(), pair2.getValue());
                inputIndexReader.moveToNext();
                return false;
            }
        } else {
            finalIndexWriter.put(pair1.getKey(), pair1.getValue());
            return true;
        }
    }

    /**
     * There will be no call to accept more pairs into index. So all pairs from
     * input reader have to be copied to output index.
     */
    public void finishMerging() {
        while (inputIndexReader.readCurrent().isPresent()) {
            final Pair<K, V> pair = inputIndexReader.readCurrent().get();
            finalIndexWriter.put(pair.getKey(), pair.getValue());
            inputIndexReader.moveToNext();
        }
    }

    @Override
    public void close() {
        finishMerging();
        finalIndexWriter.close();
    }

}
