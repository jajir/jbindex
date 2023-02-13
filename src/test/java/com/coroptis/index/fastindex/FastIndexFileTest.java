package com.coroptis.index.fastindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptorString;

public class FastIndexFileTest {

    private final Logger logger = LoggerFactory.getLogger(FastIndex.class);

    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void test_insert_firts_page() throws Exception {
        final Directory directory = new MemDirectory();
        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {

            assertNull(fif.findSegmentId("test"));

            fif.insertSegment("ahoj", 1);
            fif.insertSegment("betka", 2);
            fif.insertSegment("cukrar", 3);
            fif.insertSegment("dikobraz", 4);

            assertEquals(3, fif.findSegmentId("cuketa"));
            assertEquals(3, fif.findSegmentId("bziknout"));
            /*
             * Inserting of new higher key, should not add segment. In should
             * update key in higher segment key.
             */
            assertEquals(4, fif.insertKeyToSegment("kachna"));

            fif.getPagesAsStream().forEach(p -> {
                logger.debug("Segment '{}'", p.toString());
            });
        }

        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {
            assertEquals(3, fif.findSegmentId("cuketa"));
            assertEquals(3, fif.findSegmentId("bziknout"));
            assertEquals(4, fif.findSegmentId("kachna"));
            assertEquals(2, fif.findSegmentId("backora"));
            assertEquals(1, fif.findSegmentId("ahoj"));
            assertEquals(1, fif.findSegmentId("aaaaa"));
            assertEquals(1, fif.findSegmentId("a"));

            /*
             * Verify that pages are returned as sorted stream.
             */
            final List<Pair<String, Integer>> list = fif.getPagesAsStream()
                    .collect(Collectors.toList());
            assertEquals(4, list.size());
            assertEquals(Pair.of("ahoj", 1), list.get(0));
            assertEquals(Pair.of("betka", 2), list.get(1));
            assertEquals(Pair.of("cukrar", 3), list.get(2));
            assertEquals(Pair.of("kachna", 4), list.get(3));
        }
    }

}
