package com.coroptis.index.it;

import org.junit.jupiter.api.Test;

import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.MemDirectory;
import com.hestiastore.index.sst.Index;
import com.hestiastore.index.sst.IndexConfiguration;
import com.hestiastore.index.sst.SegmentWindow;

public class ExampleIT {

    @Test
    void test_create_1() {
        // Create an in-memory file system abstraction
        final Directory directory = new MemDirectory();

        // Prepare index configuration
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index") //
                .build();

        // create new index
        Index<String, String> index = Index.<String, String>create(directory,
                conf);

        // Do some work with the index
        index.put("Hello", "World");

        String value = index.get("Hello");
        System.out.println("Value for 'Hello': " + value);

        index.close();

        reopen(directory);
    }

    private void reopen(final Directory directory) {
        IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index") //
                .build();

        Index<String, String> index = Index.<String, String>open(directory,
                conf);

        index.getStream().forEach(entry -> {
            System.out.println("Entry: " + entry);
        });

        SegmentWindow window = SegmentWindow.of(1000, 10);

        index.getStream(window).forEach(entry -> {
            System.out.println("Entry: " + entry);
        });

        index.flush();

        index.checkAndRepairConsistency();

        index.compact();
    }

}
