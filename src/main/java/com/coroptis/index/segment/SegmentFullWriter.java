package com.coroptis.index.segment;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.scarceindex.ScarceIndexWriter;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * Allows to rewrite whole main sst index file and build new scarce index.
 */
public class SegmentFullWriter<K, V> implements PairWriter<K, V> {

    private final Segment<K, V> segment;
    private final AtomicLong cx = new AtomicLong(0L);
    private final ScarceIndexWriter<K> scarceWriter;
    private final SstFileWriter<K, V> indexWriter;
    private Pair<K, V> previousPair = null;

    SegmentFullWriter(final Segment<K, V> segment) {
        this.segment = Objects.requireNonNull(segment);
        this.scarceWriter = segment.getTempScarceIndex().openWriter();
        this.indexWriter = segment.getTempIndexFile().openWriter();

    }

    @Override
    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);

        if (previousPair != null) {
            final long i = cx.getAndIncrement();
            /*
             * Write first pair end every nth pair.
             */
            if (i % segment.getMaxNumberOfKeysInIndexPage() == 0) {
                final int position = indexWriter.put(previousPair, true);
                scarceWriter.put(Pair.of(previousPair.getKey(), position));
            } else {
                indexWriter.put(previousPair);
            }
        }

        previousPair = pair;
    }

    @Override
    public void close() {
        if (previousPair != null) {
            // write last pair to scarce index
            final int position = indexWriter.put(previousPair, true);
            scarceWriter.put(Pair.of(previousPair.getKey(), position));
            cx.getAndIncrement();
        }
        scarceWriter.close();
        indexWriter.close();
        segment.finishFullWrite(cx.get());
    }

}