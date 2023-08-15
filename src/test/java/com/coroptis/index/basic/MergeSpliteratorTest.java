package com.coroptis.index.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

public class MergeSpliteratorTest {

    @SuppressWarnings("unchecked")
    final PairIterator<String, String> pairReader1 = mock(PairIterator.class);

    @SuppressWarnings("unchecked")
    final PairIterator<String, String> pairReader2 = mock(PairIterator.class);

    @SuppressWarnings("unchecked")
    final PairIterator<Integer, String> file1Reader = mock(PairIterator.class);

    @Test
    public void test_1_valid_in_reader_1() throws Exception {

        final List<PairIterator<Integer, String>> readers = Stream
                .of(file1Reader).collect(Collectors.toList());
        final MergeSpliterator<Integer, String> pok = new MergeSpliterator<Integer, String>(
                readers, (int1, int2) -> int1.compareTo(int2),
                (key, val1, val2) -> val1);

        when(file1Reader.readCurrent()).thenReturn(Optional
                .of(new Pair<Integer, String>(Integer.valueOf(4), "test1")));
        assertTrue(pok.tryAdvance(pair -> {
            assertEquals(Integer.valueOf(4), pair.getKey());
            assertEquals("test1", pair.getValue());
        }));
        verify(file1Reader, times(1)).next();

        when(file1Reader.readCurrent()).thenReturn(Optional.empty());
        assertFalse(pok.tryAdvance(pair -> {
            fail();
        }));

    }

}
