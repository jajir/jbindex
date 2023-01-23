package com.coroptis.index.jbindex;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeDescriptorInteger;
import com.coroptis.index.type.ConvertorFromBytes;

public class IndexDesc {

    final static String INDEX_DESCRIPTION_FILE = "desc.dat";

    private final TypeDescriptorInteger integerTypeDescriptor = new TypeDescriptorInteger();

    private int writtenKeyCount = 0;

    private int writtenBlockCount = 0;

    private int blockSize;

    private final Directory directory;

    public IndexDesc(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
    }

    public static IndexDesc load(final Directory directory) {
        final IndexDesc out = new IndexDesc(directory);
        out.load();
        return out;
    }

    public static IndexDesc create(final Directory directory, final int defaultBlockSize) {
        final IndexDesc out = new IndexDesc(directory);
        out.blockSize = defaultBlockSize;
        return out;
    }

    public void writeDescriptionFile() {
        try (final FileWriter desc = directory.getFileWriter(INDEX_DESCRIPTION_FILE)) {
            final ConvertorToBytes<Integer> intWriter = integerTypeDescriptor.getConvertorTo();
            desc.write(intWriter.toBytes(blockSize));
            desc.write(intWriter.toBytes(writtenBlockCount));
            desc.write(intWriter.toBytes(writtenKeyCount));
        }
    }

    private void load() {
        try (final FileReader desc = directory.getFileReader(INDEX_DESCRIPTION_FILE)) {
            final ConvertorFromBytes<Integer> intReader = integerTypeDescriptor.getConvertorFrom();
            final byte[] data = new byte[4];
            desc.read(data);
            blockSize = intReader.fromBytes(data);

            desc.read(data);
            writtenBlockCount = intReader.fromBytes(data);

            desc.read(data);
            writtenKeyCount = intReader.fromBytes(data);
        }
    }

    void incrementWrittenKeyCount() {
        writtenKeyCount++;
    }

    void incrementBlockCount() {
        writtenBlockCount++;
    }

    boolean isBlockStart() {
        return writtenKeyCount % blockSize == 0;
    }

    /**
     * @return the writtenKeyCount
     */
    public int getWrittenKeyCount() {
        return writtenKeyCount;
    }

    /**
     * @return the writtenBlockCount
     */
    public int getWrittenBlockCount() {
        return writtenBlockCount;
    }

    /**
     * @return the blockSize
     */
    public int getBlockSize() {
        return blockSize;
    }

}
