package com.coroptis.index.fastindex;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.type.TypeDescriptorInteger;
import com.coroptis.index.type.TypeDescriptorString;

public class ScarceIndexFileSanityCheckTest {
    private final TypeDescriptorString stringTd = new TypeDescriptorString();
    private final TypeDescriptorInteger integerTd = new TypeDescriptorInteger();
    private final Directory directory = new MemDirectory();

    @Test
    public void test_sanityCheck() throws Exception {
        final SortedDataFile<String, Integer> sdf = new SortedDataFile<>(
                directory, "index.map", integerTd.getTypeWriter(),
                integerTd.getTypeReader(), stringTd.getComparator(),
                stringTd.getConvertorFromBytes(),
                stringTd.getConvertorToBytes());

        try (final SortedDataFileWriter<String, Integer> writer = sdf
                .openWriter()) {
            writer.put(Pair.of("aaa", 1));
            writer.put(Pair.of("bbb", 2));
            writer.put(Pair.of("ccc", 3));
            writer.put(Pair.of("ddd", 4));
            writer.put(Pair.of("eee", 5));
            writer.put(Pair.of("fff", 3));
        }

        assertThrows(IllegalStateException.class, () -> {
            try (final ScarceIndexFile<String> fif = new ScarceIndexFile<>(
                    directory, stringTd)) {
            }
        }, "Unable to load scarce index, sanity check failed.");

    }

}
