package com.coroptis.index.segment;

import java.io.File;
import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Props;

public class SegmentStatsManager {

    private final static String NUMBER_OF_KEYS_IN_CACHE = "numberOfKeysInCache";
    private final static String NUMBER_OF_KEYS_IN_INDEX = "numberOfKeysInIndex";
    private final static String PROPERTIES_FILENAME_EXTENSION = File.separator
            + "properties";

    private final SegmentId id;
    private final Props props;

    SegmentStatsManager(final Directory directory, final SegmentId id) {
        Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.props = new Props(directory, getPropertiesFilename());
    }

    private String getPropertiesFilename() {
        return id.getName() + PROPERTIES_FILENAME_EXTENSION;
    }

    public SegmentStats getSegmentStats() {
        return new SegmentStats(props.getLong(NUMBER_OF_KEYS_IN_CACHE),
                props.getLong(NUMBER_OF_KEYS_IN_INDEX));
    }

    public void setNumberOfKeysInCache(final long numberOfKeysInCache) {
        props.setLong(NUMBER_OF_KEYS_IN_CACHE, numberOfKeysInCache);
    }

    public void setNumberOfKeysInIndex(final long numberOfKeysInIndex) {
        props.setLong(NUMBER_OF_KEYS_IN_INDEX, numberOfKeysInIndex);
    }

    void flush() {
        props.writeData();
    }

}
