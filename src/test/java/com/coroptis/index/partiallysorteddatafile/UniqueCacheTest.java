package com.coroptis.index.partiallysorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.basic.ValueMerger;

public class UniqueCacheTest {

    private final ValueMerger<Integer, String> concat = new ValueMerger<Integer, String>() {

        @Override
        public String merge(Integer key, String value1, String value2) {
            return value1 + value2;
        }
    };

    private final UniqueCache<Integer, String> cache = new UniqueCache<>(concat);

    @Test
    public void test_basic_function() throws Exception {
        cache.add(Pair.of(10, "hello"));
        cache.add(Pair.of(13, "my"));
        cache.add(Pair.of(15, "dear"));

        final List<Pair<Integer, String>> out = cache.toList();
        assertEquals(3, cache.size());
        assertEquals(Pair.of(10, "hello"), out.remove(0));
        assertEquals(Pair.of(13, "my"), out.remove(0));
        assertEquals(Pair.of(15, "dear"), out.remove(0));
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void test_basic_function_different_order() throws Exception {
        cache.add(Pair.of(15, "dear"));
        cache.add(Pair.of(13, "my"));
        cache.add(Pair.of(-199, "hello"));

        final List<Pair<Integer, String>> out = cache.toList();
        assertEquals(3, cache.size());
        assertEquals(Pair.of(-199, "hello"), out.remove(0));
        assertEquals(Pair.of(13, "my"), out.remove(0));
        assertEquals(Pair.of(15, "dear"), out.remove(0));
        cache.clear();
        assertEquals(0, cache.size());
    }

    /**
     * Verify that merging is called in right time.
     * 
     * @throws Exception
     */
    @Test
    public void test_merging() throws Exception {
        cache.add(Pair.of(10, "hello"));
        cache.add(Pair.of(10, "my"));
        cache.add(Pair.of(10, "dear"));

        final List<Pair<Integer, String>> out = cache.toList();
        assertEquals(1, cache.size());
        assertEquals(Pair.of(10, "hellomydear"), out.remove(0));
        cache.clear();
        assertEquals(0, cache.size());
    }

}
