package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorByte;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IntegrationSortedDataFileWriterTest {

    private final static int DISK_IO_BUFFER_SIZE = 1024;
    private final static String FILE_NAME = "pok.dat";
    private final TypeDescriptorByte byteTd = new TypeDescriptorByte();
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void read_incorrect_insert_order_mem() throws Exception {
        final Directory directory = new MemDirectory();
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(directory,
                FILE_NAME, stringTd.getConvertorToBytes(),
                Comparator.naturalOrder(), byteTd.getTypeWriter(),
                DISK_IO_BUFFER_SIZE)) {
            assertEquals(0,
                    siw.put(new Pair<String, Byte>("aaabbb", (byte) 1), true));
            assertThrows(IllegalArgumentException.class, () -> {
                siw.put(new Pair<String, Byte>("aaa", (byte) 0), false);
            });
        }
    }

    @Test
    public void test_invalidOrder() throws Exception {
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                new MemDirectory(), FILE_NAME, stringTd.getConvertorToBytes(),
                Comparator.naturalOrder(), byteTd.getTypeWriter(),
                DISK_IO_BUFFER_SIZE)) {
            siw.put(new Pair<String, Byte>("aaa", (byte) 0));
            siw.put(new Pair<String, Byte>("abbb", (byte) 1));
            assertThrows(IllegalArgumentException.class,
                    () -> siw.put(new Pair<String, Byte>("aaaa", (byte) 2)));
        }
    }

    @Test
    public void test_duplicatedValue() throws Exception {
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                new MemDirectory(), FILE_NAME, stringTd.getConvertorToBytes(),
                Comparator.naturalOrder(), byteTd.getTypeWriter(),
                DISK_IO_BUFFER_SIZE)) {
            siw.put(new Pair<String, Byte>("aaa", (byte) 0));
            siw.put(new Pair<String, Byte>("abbb", (byte) 1));
            assertThrows(IllegalArgumentException.class,
                    () -> siw.put(new Pair<String, Byte>("abbb", (byte) 2)));
        }
    }

    @Test
    public void test_null_key() throws Exception {
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                new MemDirectory(), FILE_NAME, stringTd.getConvertorToBytes(),
                Comparator.naturalOrder(), byteTd.getTypeWriter(),
                DISK_IO_BUFFER_SIZE)) {

            assertThrows(NullPointerException.class,
                    () -> siw.put(new Pair<String, Byte>(null, (byte) 0)));
        }

    }

}
