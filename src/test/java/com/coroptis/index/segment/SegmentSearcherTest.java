package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.PairReader;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.sstfile.SstFile;

@ExtendWith(MockitoExtension.class)
public class SegmentSearcherTest {

    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentConf segmentConf;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SstFile<Integer, String> sstFile;

    @Mock
    private SegmentIndexSearcherSupplier<Integer, String> segmentIndexSearcherSupplier;

    @Mock
    private SegmentIndexSearcher<Integer, String> segmentIndexSearcher;

    @Mock
    private PairReader<Integer, String> pairReader;

    @Mock
    private SegmentDataProvider<Integer, String> segmentCacheDataProvider;

    @Mock
    private SegmentDeltaCache<Integer, String> deltaCache;

    @Mock
    private BloomFilter<Integer> bloomFilter;

    @Mock
    private ScarceIndex<Integer> scarceIndex;

    @Mock
    private Directory directory;

    @Test
    public void test_simple_get_by_key() throws Exception {
        final MockVersionProvider versionProvider = new MockVersionProvider();
        when(segmentIndexSearcherSupplier.get())
                .thenReturn(segmentIndexSearcher);
        prepareOpeningSearcher();
        when(segmentCacheDataProvider.getSegmentDeltaCache())
                .thenReturn(deltaCache);
        when(segmentCacheDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentCacheDataProvider.getScarceIndex()).thenReturn(scarceIndex);
        when(segmentIndexSearcher.search(37, 0)).thenReturn("hello");
        try (SegmentSearcherOL<Integer, String> searcher = new SegmentSearcherOL<>(
                segmentFiles, segmentConf, versionProvider,
                segmentPropertiesManager, segmentIndexSearcherSupplier,
                segmentCacheDataProvider)) {
            assertEquals("hello", searcher.get(37));
            assertEquals(null, searcher.get(5));
            assertEquals("hello", searcher.get(37));
        }
    }

    private void prepareOpeningSearcher() {
        when(segmentFiles.getId()).thenReturn(SegmentId.of(7));
//        when(segmentFiles.getKeyTypeDescriptor()).thenReturn(tdi);
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(tds);
//        when(segmentFiles.getCacheSstFile()).thenReturn(sstFile);
//        when(sstFile.openReader()).thenReturn(pairReader);
//        when(pairReader.read()).thenReturn(Pair.of(37, "hello"))
//                .thenReturn(null);
//        when(segmentFiles.getDirectory()).thenReturn(directory);
//        when(segmentFiles.getScarceFileName())
//                .thenReturn("segment-00007.scarce");
//        when(segmentFiles.getBloomFilterFileName())
//                .thenReturn("segment-00007.bloom-filter");        
    }

    static class MockVersionProvider
            implements OptimisticLockObjectVersionProvider {

        private int version;

        void changeVerson() {
            version++;
        }

        @Override
        public int getVersion() {
            return version;
        }

    }

}
