package com.coroptis.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PropsTest {

    private final MemDirectory directory = new MemDirectory();

    @Test
    void test_storing_data() throws Exception {
        final Props props = new Props(directory, "pok1.properties");
        props.setInt("test0", 21);
        props.setLong("test1", 42);
        assertEquals(21, props.getInt("test0"));
        assertEquals(42, props.getLong("test1"));
    }

    @Test
    void test_read_default_values() throws Exception {
        final Props props = new Props(directory, "pok1.properties");
        assertEquals(0, props.getInt("test0"));
        assertEquals(0, props.getLong("test1"));
    }

    @Test
    void test_write_and_read() throws Exception {
        final Props props1 = new Props(directory, "pok1.properties");
        props1.setInt("test0", 21);
        props1.setLong("test1", 42);
        props1.writeData();

        final Props props2 = new Props(directory, "pok1.properties");
        assertEquals(21, props2.getInt("test0"));
        assertEquals(42, props2.getLong("test1"));
    }

}
