package com.coroptis.index.sorteddatafile;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.FileWriter;

public class CompressingWriter implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(CompressingWriter.class);
    private final FileWriter writer;
    private final DataCompressor dataCompressor;
    private long position = 0;

    CompressingWriter(final FileWriter writer, final DataCompressor dataCompressor) {
        this.writer = Objects.requireNonNull(writer, "writer must not be null");
        this.dataCompressor = Objects.requireNonNull(dataCompressor, "dataCompressor must not be null");
    }

    long write(final byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        byte[] compressed = dataCompressor.compress(bytes);
        writer.write(compressed);
        long out = position;
        position += compressed.length;
        float ratio = (float) compressed.length / (float) bytes.length * 100F;
        logger.debug("Data was compressed from '{}' bytes to '{}' bytes, it's about {}%", bytes.length,
                compressed.length,
                ratio);
        return out;
    }

    @Override
    public void close() {
        writer.close();
    }

}
