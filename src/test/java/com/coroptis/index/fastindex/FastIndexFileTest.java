package com.coroptis.index.fastindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

    private Directory directory;

    @BeforeEach
    public void prepareData() {
        directory = new MemDirectory();
        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {
            fif.insertSegment("ahoj", 1);
            fif.insertSegment("betka", 2);
            fif.insertSegment("cukrar", 3);
            fif.insertSegment("dikobraz", 4);
            /*
             * Inserting of new higher key, should not add segment. In should
             * update key in higher segment key.
             */
            assertEquals(4, fif.insertKeyToSegment("kachna"));
        }
    }

    @AfterEach
    public void cleanData() {
        directory = null;
    }

    @Test
    public void test_constructor_empty_directory() throws Exception {
        assertThrows(NullPointerException.class, () -> {
            try (final FastIndexFile<String> fif = new FastIndexFile<>(null,
                    stringTd)) {
            }
        });
    }

    @Test
    public void test_constructor_empty_keyTypeDescriptor() throws Exception {
        assertThrows(NullPointerException.class, () -> {
            try (final FastIndexFile<String> fif = new FastIndexFile<>(
                    directory, null)) {
            }
        });
    }

    @Test
    public void test_insert_key_into_higher_segment() throws Exception {
        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {
            assertEquals(4, fif.insertKeyToSegment("zzz"));
            assertEquals(4, fif.findSegmentId("zzz"));
            assertEquals(4, fif.findSegmentId("zzz"));
            /*
             * Verify that higher page key was updated.
             */
            final List<Pair<String, Integer>> list = fif.getPagesAsStream()
                    .collect(Collectors.toList());
            assertEquals(Pair.of("zzz", 4), list.get(3));
        }
    }

    @Test
    public void test_print_data() throws Exception {
        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {
            fif.getPagesAsStream().forEach(p -> {
                logger.debug("Segment '{}'", p.toString());
            });
        }
    }

    @Test
    public void test_number_of_segments() throws Exception {
        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {
            assertEquals(4, fif.getPagesAsStream().count());
        }
    }

    @Test
    public void test_correct_page_order() throws Exception {
        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {
            /*
             * Verify that pages are returned as sorted stream.
             */
            final List<Pair<String, Integer>> list = fif.getPagesAsStream()
                    .collect(Collectors.toList());
            assertEquals(Pair.of("ahoj", 1), list.get(0));
            assertEquals(Pair.of("betka", 2), list.get(1));
            assertEquals(Pair.of("cukrar", 3), list.get(2));
            assertEquals(Pair.of("kachna", 4), list.get(3));
        }
    }

    @Test
    public void test_segment_finding() throws Exception {
        try (final FastIndexFile<String> fif = new FastIndexFile<>(directory,
                stringTd)) {
            assertEquals(3, fif.findSegmentId("cuketa"));
            assertEquals(3, fif.findSegmentId("bziknout"));
            assertEquals(4, fif.findSegmentId("kachna"));
            assertEquals(2, fif.findSegmentId("backora"));
            assertEquals(1, fif.findSegmentId("ahoj"));
            assertEquals(1, fif.findSegmentId("aaaaa"));
            assertEquals(1, fif.findSegmentId("a"));

            assertNull(fif.findSegmentId("zzz"));
        }
    }

}
