package com.coroptis.index.unsorteddatafile;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.coroptis.index.DataFileIterator;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.rigidindex.IndexConfiguration;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.type.TypeDescriptorInteger;
import com.coroptis.index.type.TypeDescriptorString;

public class UnsortedFileTest {

    @Test
    public void test_in_mem_unsorted_index() throws Exception {
        final Directory dir = new MemDirectory();

        final IndexConfiguration<Integer, String> configuration = new IndexConfiguration<>(dir,
                new TypeDescriptorInteger(), new TypeDescriptorString());

        final UnsortedDataFile<Integer, String> unsorted = configuration.getUnsortedFile("duck");
        assertNotNull(unsorted);

        try (final UnsortedDataFileWriter<Integer, String> writer = unsorted.openWriter()) {
            writer.put(Pair.of(4, "here"));
            writer.put(Pair.of(-12, "we"));
            writer.put(Pair.of(98, "go"));
        }

        try (final DataFileIterator<Integer, String> reader = unsorted.openIterator()) {
            while (reader.hasNext()) {
                System.out.println(reader.readCurrent().get());
                reader.next();
            }
        }

        try (final UnsortedDataFileStreamer<Integer, String> streamer = unsorted.openStreamer()) {
            streamer.stream().forEach(pair -> {
                System.out.println(pair);
            });
        }

    }

}
