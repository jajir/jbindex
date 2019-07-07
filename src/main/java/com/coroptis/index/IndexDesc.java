package com.coroptis.index;

import java.util.Objects;

import com.coroptis.index.storage.Directory;
import com.coroptis.index.storage.FileReader;
import com.coroptis.index.storage.FileWriter;
import com.coroptis.index.type.IntegerTypeDescriptor;
import com.coroptis.index.type.TypeArrayWriter;
import com.coroptis.index.type.TypeRawArrayReader;

public class IndexDesc {

    final static String INDEX_DESCRIPTION_FILE = "desc.dat";

    private final IntegerTypeDescriptor integerTypeDescriptor = new IntegerTypeDescriptor();

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
	    final TypeArrayWriter<Integer> intWriter = integerTypeDescriptor.getArrayWriter();
	    desc.write(intWriter.toBytes(blockSize));
	    desc.write(intWriter.toBytes(writtenBlockCount));
	    desc.write(intWriter.toBytes(writtenKeyCount));
	}
    }

    private void load() {
	try (final FileReader desc = directory.getFileReader(INDEX_DESCRIPTION_FILE)) {
	    final TypeRawArrayReader<Integer> intReader = integerTypeDescriptor.getRawArrayReader();
	    final byte[] data = new byte[4];
	    desc.read(data);
	    blockSize = intReader.read(data);

	    desc.read(data);
	    writtenBlockCount = intReader.read(data);

	    desc.read(data);
	    writtenKeyCount = intReader.read(data);
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
