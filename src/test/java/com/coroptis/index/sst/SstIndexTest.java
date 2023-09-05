package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.SegmentId;

public class SstIndexTest {

    private final Logger logger = LoggerFactory.getLogger(SstIndexTest.class);

    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorString tds = new TypeDescriptorString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void testBasic() throws Exception {

        final SstIndexImpl<Integer, String> sstIndex = SstIndexImpl
                .<Integer, String>builder().withDirectory(directory)
                .withKeyTypeDescriptor(tdi).withValueTypeDescriptor(tds)
                .withMaxNumberOfKeysInSegment(4)
                .withMaxNumberOfKeysInSegmentCache(1)
                .withMaxNumberOfKeysInSegmentIndexPage(2)
                .withMaxNumberOfKeysInCache(2).build();

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"));
        data.stream().forEach(sstIndex::put);

        sstIndex.forceCompact();

        data.stream().forEach(pair -> {
            final String value = sstIndex.get(pair.getKey());
            assertEquals(pair.getValue(), value);
        });

        assertEquals(5, numberOfFilesInDirectoryP(directory));
    }

    private int numberOfFilesInDirectoryP(final Directory directory) {
        final AtomicInteger cx = new AtomicInteger(0);
        directory.getFileNames().forEach(fileName -> {
            logger.debug("Found file name {}", fileName);
            cx.incrementAndGet();
        });
        return cx.get();
    }

}
