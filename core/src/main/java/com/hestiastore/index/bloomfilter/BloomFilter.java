package com.hestiastore.index.bloomfilter;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hestiastore.index.CloseableResource;
import com.hestiastore.index.datatype.ConvertorToBytes;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.FileReader;
import com.hestiastore.index.directory.FileWriter;

public class BloomFilter<K> implements CloseableResource {

    private final static String TEMP_FILE_EXTENSION = ".tmp";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Directory directory;

    private final String bloomFilterFileName;

    private final ConvertorToBytes<K> convertorToBytes;

    private final BloomFilterStats bloomFilterStats;

    private final int numberOfHashFunctions;

    private final int indexSizeInBytes;

    private final String relatedObjectName;

    private Hash hash;

    private final int diskIoBufferSize;

    public static <M> BloomFilterBuilder<M> builder() {
        return new BloomFilterBuilder<>();
    }

    BloomFilter(final Directory directory, final String bloomFilterFileName,
            final int numberOfHashFunctions, final int indexSizeInBytes,
            final ConvertorToBytes<K> convertorToBytes,
            final String relatedObjectName, final int diskIoBufferSize) {
        this.directory = Objects.requireNonNull(directory,
                "Directory is required");
        this.bloomFilterFileName = Objects.requireNonNull(bloomFilterFileName,
                "Bloom filter file name is required");
        this.convertorToBytes = Objects.requireNonNull(convertorToBytes,
                "Convertor to bytes is required");
        this.relatedObjectName = Objects.requireNonNull(relatedObjectName,
                "Bloom filter related object name is required");
        this.indexSizeInBytes = indexSizeInBytes;
        this.numberOfHashFunctions = numberOfHashFunctions;
        this.bloomFilterStats = new BloomFilterStats();
        this.diskIoBufferSize = diskIoBufferSize;
        if (numberOfHashFunctions <= 0) {
            throw new IllegalArgumentException(
                    String.format("Number of hash function cant be '0'"));
        }
        if (isExists() && indexSizeInBytes > 0) {
            try (FileReader reader = directory
                    .getFileReader(bloomFilterFileName, diskIoBufferSize)) {
                final byte[] data = new byte[indexSizeInBytes];
                final int readed = reader.read(data);
                if (indexSizeInBytes != readed) {
                    throw new IllegalStateException(String.format(
                            "Bloom filter data from file '%s' wasn't loaded,"
                                    + " index expected size is '%s' but '%s' was loaded",
                            bloomFilterFileName, indexSizeInBytes, readed));
                }
                hash = new Hash(new BitArray(data), numberOfHashFunctions);
            }
        } else {
            hash = null;
        }
        logger.debug("Opening bloom filter for '{}'", relatedObjectName);
    }

    public BloomFilterWriter<K> openWriter() {
        return new BloomFilterWriter<>(convertorToBytes,
                new Hash(new BitArray(indexSizeInBytes), numberOfHashFunctions),
                this);
    }

    void setNewHash(final Hash newHash) {
        Objects.requireNonNull(newHash, "New hash can't be null");
        this.hash = newHash;
        try (FileWriter writer = directory.getFileWriter(getTempFileName(),
                Directory.Access.OVERWRITE, diskIoBufferSize)) {
            writer.write(hash.getData());
        }
        directory.renameFile(getTempFileName(), bloomFilterFileName);
    }

    private boolean isExists() {
        return directory.isFileExists(bloomFilterFileName);
    }

    private final String getTempFileName() {
        return bloomFilterFileName + TEMP_FILE_EXTENSION;
    }

    /**
     * Get information if key is not stored in index. False doesn't mean that
     * key is stored in index it means that it's not sure.
     * 
     * @param key
     * @return Return <code>true</code> when it's sure that record is not stored
     *         in index. Otherwise return <code>false</false>
     */
    public boolean isNotStored(final K key) {
        if (hash == null) {
            bloomFilterStats.increment(false);
            return false;
        } else {
            final boolean out = hash.isNotStored(convertorToBytes.toBytes(key));
            bloomFilterStats.increment(out);
            return out;
        }
    }

    public BloomFilterStats getStatistics() {
        return bloomFilterStats;
    }

    public void incrementFalsePositive() {
        bloomFilterStats.incrementFalsePositive();
    }

    public long getNumberOfHashFunctions() {
        return numberOfHashFunctions;
    }

    public long getIndexSizeInBytes() {
        return indexSizeInBytes;
    }

    @Override
    public void close() {
        logger.debug("Closing bloom filter for '{}'. {}", relatedObjectName,
                bloomFilterStats.getStatsString());
    }

}
