package com.coroptis.index.unsorteddatafile;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.DataFileIterator;
import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.type.TypeDescriptor;
import com.coroptis.index.type.TypeDescriptorInteger;
import com.coroptis.index.type.TypeDescriptorString;

public class UnsortedFileTest {

    private final Logger logger = LoggerFactory
            .getLogger(UnsortedFileTest.class);

    @Test
    public void test_in_mem_unsorted_index() throws Exception {
        final Directory dir = new MemDirectory();
        final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
        final TypeDescriptor<String> tds = new TypeDescriptorString();

        final UnsortedDataFile<Integer, String> unsorted = new UnsortedDataFile<>(
                dir, "duck", tdi.getTypeWriter(), tds.getTypeWriter(),
                tdi.getTypeReader(), tds.getTypeReader());
        assertNotNull(unsorted);

        try (final PairWriter<Integer, String> writer = unsorted.openWriter()) {
            writer.put(Pair.of(4, "here"));
            writer.put(Pair.of(-12, "we"));
            writer.put(Pair.of(98, "go"));
        }

        try (final DataFileIterator<Integer, String> reader = unsorted
                .openIterator()) {
            while (reader.hasNext()) {
                logger.debug(reader.readCurrent().get().toString());
                reader.next();
            }
        }

        try (final UnsortedDataFileStreamer<Integer, String> streamer = unsorted
                .openStreamer()) {
            streamer.stream().forEach(pair -> {
                logger.debug(pair.toString());
            });
        }

    }

}
