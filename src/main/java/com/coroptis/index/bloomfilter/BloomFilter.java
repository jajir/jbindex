package com.coroptis.index.bloomfilter;

import java.util.Objects;

import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.directory.FileWriter;

public class BloomFilter<K> {

    private final Directory directory;

    private final String bloomFilterFileName;

    private final ConvertorToBytes<K> convertorToBytes;

    private final BloomFilterStats bloomFilterStats;

    private final int numberOfHashFunctions;

    private final int indexSizeInBytes;

    private Hash hash;

    public static <M> BloomFilterBuilder<M> builder() {
        return new BloomFilterBuilder<>();
    }

    BloomFilter(final Directory directory, final String bloomFilterFileName,
            final int numberOfHashFunctions, final int indexSizeInBytes,
            final ConvertorToBytes<K> convertorToBytes) {
        this.directory = Objects.requireNonNull(directory,
                "Directory is required");
        this.bloomFilterFileName = Objects.requireNonNull(bloomFilterFileName,
                "Bloom filter file name is required");
        this.convertorToBytes = Objects.requireNonNull(convertorToBytes,
                "Convertor to bytes is required");
        this.indexSizeInBytes = indexSizeInBytes;
        this.numberOfHashFunctions = numberOfHashFunctions;
        this.bloomFilterStats = new BloomFilterStats();
        if (isExists()) {
            try (FileReader reader = directory
                    .getFileReader(bloomFilterFileName)) {
                final byte[] data = new byte[indexSizeInBytes];
                if (indexSizeInBytes != reader.read(data)) {
                    throw new IllegalStateException(String.format(
                            "Bloom filter data from file '%s' wasn't loaded",
                            bloomFilterFileName));
                }
                hash = new Hash(new BitArray(data), numberOfHashFunctions);
            }
        } else {
            hash = null;
        }
    }

    public BloomFilterWriter<K> openWriter() {
        return new BloomFilterWriter<>(convertorToBytes,
                new Hash(new BitArray(indexSizeInBytes), numberOfHashFunctions),
                this);
    }

    void setNewHash(final Hash newHash) {
        Objects.requireNonNull(newHash, "New hash can't be null");
        this.hash = newHash;
        try (FileWriter writer = directory.getFileWriter(bloomFilterFileName)) {
            writer.write(hash.getData());
        }

    }

    private boolean isExists() {
        return directory.isFileExists(bloomFilterFileName);
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

    public String getStatsString() {
        return bloomFilterStats.getStatsString();
    }

    public void incrementFalsePositive() {
        bloomFilterStats.incrementFalsePositive();
    }

}
