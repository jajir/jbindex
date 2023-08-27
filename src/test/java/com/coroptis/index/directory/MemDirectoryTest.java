package com.coroptis.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory.Access;

public class MemDirectoryTest {

    private final byte[] name = "Karel".getBytes();
    private final byte[] surname = "Novotny".getBytes();

    @Test
    public void test_write_and_append() throws Exception {
        final MemDirectory directory = new MemDirectory();
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(name);
        fw.close();

        fw = directory.getFileWriter("pok", Access.APPEND);
        fw.write(surname);
        fw.close();

        final FileReader fr = directory.getFileReader("pok");
        byte[] read = new byte[name.length + surname.length];
        fr.read(read);

        assertEquals("KarelNovotny", new String(read));
    }

    @Test
    public void test_write_and_overwrite() throws Exception {
        final MemDirectory directory = new MemDirectory();
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(name);
        fw.close();

        fw = directory.getFileWriter("pok", Access.OVERWRITE);
        fw.write(surname);
        fw.close();

        final FileReader fr = directory.getFileReader("pok");
        byte[] read = new byte[surname.length];
        fr.read(read);

        assertEquals("Novotny", new String(read));
    }

    @Test
    void test_fileExists() throws Exception {
        final MemDirectory directory = new MemDirectory();
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(name);
        fw.close();

        assertTrue(directory.isFileExists("pok"));
        assertFalse(directory.isFileExists("anotherOne"));
    }

}
