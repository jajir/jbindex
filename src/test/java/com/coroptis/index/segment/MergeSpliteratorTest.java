package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorReader;
import com.coroptis.index.PairReader;
import com.coroptis.index.datatype.TypeDescriptorInteger;

public class MergeSpliteratorTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Mock
    private PairReader<Integer, Integer> sstReader;

    @Mock
    private PairReader<Integer, Integer> cacheReader;

    private Stream<Pair<Integer, Integer>> stream;

    @Test
    void test_empty_stream() throws Exception {
        when(sstReader.read()).thenReturn(null);
        when(cacheReader.read()).thenReturn(null);
        stream = makeStream();

        assertEquals(0, stream.count());
    }

    @Test
    void test_cache_is_empty() throws Exception {
        when(sstReader.read()).thenReturn(Pair.of(1,1)).thenReturn(Pair.of(2,2)).thenReturn(null);
        when(cacheReader.read()).thenReturn(null);
        stream = makeStream();

        final List<Pair<Integer,Integer>> list = stream.collect(Collectors.toList());
        assertEquals(2, list.size());
    }

    @Test
    void test_sstFile_is_empty() throws Exception {
        when(sstReader.read()).thenReturn(null);
        when(cacheReader.read()).thenReturn(Pair.of(1,1)).thenReturn(Pair.of(2,2)).thenReturn(Pair.of(3,3)).thenReturn(null);
        stream = makeStream();

        final List<Pair<Integer,Integer>> list = stream.collect(Collectors.toList());
        assertEquals(3, list.size());
    }

    @Test
    void test_merge_two_values() throws Exception {
        when(sstReader.read()).thenReturn(Pair.of(1,1)).thenReturn(null);
        when(cacheReader.read()).thenReturn(Pair.of(3,3)).thenReturn(null);
        stream = makeStream();

        final List<Pair<Integer,Integer>> list = stream.collect(Collectors.toList());
        assertEquals(2, list.size());
        assertEquals(Pair.of(1,1), list.get(0));
        assertEquals(Pair.of(3,3), list.get(1));
    }

    @Test
    void test_merge_multiple_values() throws Exception {
        when(sstReader.read()).thenReturn(Pair.of(3,3)).thenReturn(Pair.of(6,6)).thenReturn(Pair.of(8,8)).thenReturn(null);
        when(cacheReader.read()).thenReturn(Pair.of(1,1)).thenReturn(Pair.of(4,4)).thenReturn(Pair.of(5,5)).thenReturn(null);
        stream = makeStream();

        final List<Pair<Integer,Integer>> list = stream.collect(Collectors.toList());
        assertEquals(6, list.size());
        assertEquals(Pair.of(1,1), list.get(0));
        assertEquals(Pair.of(3,3), list.get(1));
        assertEquals(Pair.of(4,4), list.get(2));
        assertEquals(Pair.of(5,5), list.get(3));
        assertEquals(Pair.of(6,6), list.get(4));
        assertEquals(Pair.of(8,8), list.get(5));
    }

    @Test
    void test_merge_tombstone() throws Exception {
        when(sstReader.read()).thenReturn(Pair.of(1,1)).thenReturn(Pair.of(4,4)).thenReturn(Pair.of(5,5)).thenReturn(null);
        when(cacheReader.read()).thenReturn(Pair.of(3,3)).thenReturn(Pair.of(4,TypeDescriptorInteger.TOMBSTONE_VALUE)).thenReturn(Pair.of(8,8)).thenReturn(null);
        stream = makeStream();

        final List<Pair<Integer,Integer>> list = stream.collect(Collectors.toList());
        assertEquals(4, list.size());
        assertEquals(Pair.of(1,1), list.get(0));
        assertEquals(Pair.of(3,3), list.get(1));
        assertEquals(Pair.of(5,5), list.get(2));
        assertEquals(Pair.of(8,8), list.get(3));
    }

    @Test
    void test_merge_tombstone_and_empty_index() throws Exception {
        when(sstReader.read()).thenReturn(null);
        when(cacheReader.read()).thenReturn(Pair.of(3,3)).thenReturn(Pair.of(4,TypeDescriptorInteger.TOMBSTONE_VALUE)).thenReturn(Pair.of(8,8)).thenReturn(null);
        stream = makeStream();

        final List<Pair<Integer,Integer>> list = stream.collect(Collectors.toList());
        assertEquals(2, list.size());
        assertEquals(Pair.of(3,3), list.get(0));
        assertEquals(Pair.of(8,8), list.get(1));
    }

    @BeforeEach
    void beforeEach() {
        MockitoAnnotations.initMocks(this);
    }

    private Stream<Pair<Integer, Integer>> makeStream() {
        PairIterator<Integer, Integer> sstIterator = new PairIteratorReader<>(
                sstReader);
        PairIterator<Integer, Integer> cacheIterator = new PairIteratorReader<>(
                cacheReader);

        final MergeSpliterator<Integer, Integer> spliterator = new MergeSpliterator<>(
                sstIterator, cacheIterator, tdi, tdi);

        final Stream<Pair<Integer, Integer>> stream = StreamSupport
                .stream(spliterator, false);

        return stream;
    }

}
