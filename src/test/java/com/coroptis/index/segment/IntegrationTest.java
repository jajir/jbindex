package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IntegrationTest {

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    
    //TODO overit pocet souboru v directory

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_empty_segment_stats(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg) throws Exception {

        assertEquals(0, seg.getStats().getNumberOfKeys());

        final List<Pair<Integer, String>> list = toList(seg.getStream());
        assertEquals(0, list.size());
        
        assertNull(seg.get(1));
    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_simple(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg) throws Exception {

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "d"));
        }

        assertEquals(4, seg.getStats().getNumberOfKeys());

        final List<Pair<Integer, String>> list = toList(seg.getStream());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "b"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(Pair.of(5, "d"), list.get(3));
        assertEquals(4, list.size());
        
        assertNull(seg.get(6));
        assertEquals("a", seg.get(2));
        assertEquals("b", seg.get(3));
        assertEquals("c", seg.get(4));
        assertEquals("d", seg.get(5));
    }

    @Test
    void test_duplicities() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).build();

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(3, "bb"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "d"));
            writer.put(Pair.of(5, "dd"));
            writer.put(Pair.of(5, "ddd"));
        }

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.getStream());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "bb"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(Pair.of(5, "ddd"), list.get(3));
        assertEquals(4, list.size());
        
        assertNull(seg.get(6));
        assertEquals("a", seg.get(2));
        assertEquals("bb", seg.get(3));
        assertEquals("c", seg.get(4));
        assertEquals("ddd", seg.get(5));
    }

    @Test
    void test_write_unordered() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).build();

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(5, "d"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(5, "dd"));
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "bb"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "ddd"));
        }

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.getStream());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "bb"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(Pair.of(5, "ddd"), list.get(3));
        assertEquals(4, list.size());
        
        assertNull(seg.get(6));
        assertEquals("a", seg.get(2));
        assertEquals("bb", seg.get(3));
        assertEquals("c", seg.get(4));
        assertEquals("ddd", seg.get(5));
    }

    @Test
    void test_write_unordered_tombstone() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).build();

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(5, "d"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(5, "dd"));
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "bb"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "ddd"));
            writer.put(Pair.of(5, TypeDescriptorString.TOMBSTONE_VALUE));
        }

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.getStream());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "bb"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(3, list.size());
        
        assertNull(seg.get(5));
        assertEquals("a", seg.get(2));
        assertEquals("bb", seg.get(3));
        assertEquals("c", seg.get(4));
    }

    static Stream<Arguments> segmentProvider() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final TypeDescriptorString tds = new TypeDescriptorString();
        final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
        return Stream.of(

                arguments(tdi, tds, directory, Segment
                        .<Integer, String>builder().withDirectory(directory)
                        .withId(id).withKeyTypeDescriptor(tdi)
                        .withValueTypeDescriptor(tds)
                        .withMaxNumeberOfKeysInSegmentCache(10).build()),
                arguments(tdi, tds, directory,
                        Segment.<Integer, String>builder()
                                .withDirectory(directory).withId(id)
                                .withKeyTypeDescriptor(tdi)
                                .withValueTypeDescriptor(tds)
                                .withMaxNumeberOfKeysInSegmentCache(1).build()),
                arguments(tdi, tds, directory, Segment
                        .<Integer, String>builder().withDirectory(directory)
                        .withId(id).withKeyTypeDescriptor(tdi)
                        .withValueTypeDescriptor(tds)
                        .withMaxNumeberOfKeysInSegmentCache(2).build()));
    }

    private List<Pair<Integer, String>> toList(
            final Stream<Pair<Integer, String>> stream) {
        final ArrayList<Pair<Integer, String>> out = new ArrayList<>();
        stream.forEach(out::add);
        return out;
    }

}
