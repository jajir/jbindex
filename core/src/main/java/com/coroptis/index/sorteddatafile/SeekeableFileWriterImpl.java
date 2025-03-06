package com.coroptis.index.sorteddatafile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;

public class SeekeableFileWriterImpl implements SeekeableFileWriter {

    private final TypeDescriptor<Integer> TDI = new TypeDescriptorInteger();

    private final CompressingWriter writer;

    private long position = 0;

    private List<byte[]> buffer = new ArrayList<>();

    public SeekeableFileWriterImpl(final CompressingWriter writer) {
        this.writer = Objects.requireNonNull(writer, "CompressingWriter must not be null");
    }

    @Override
    public void write(final byte[] bytes) {
        buffer.add(bytes);
    }

    @Override
    public void write(final byte b) {
        final byte[] bytes = new byte[1];
        bytes[0] = b;
        write(bytes);
    }

    private long flush() {
        long out = position;
        int length = 0;
        for (byte[] bs : buffer) {
            length += bs.length;
        }
        byte[] toWrite = new byte[length];
        int cx = 0;
        for (byte[] bs : buffer) {
            System.arraycopy(bs, 0, toWrite, cx, bs.length);
            cx += bs.length;
        }
        position += writer.write(TDI.getConvertorToBytes().toBytes(length));
        position += writer.write(toWrite);
        return out;
    }

    @Override
    public long flushAndWrite(final byte[] bytes) {
        long out = flush();
        write(bytes);
        return out;
    }

    @Override
    public void close() {
        writer.close();
    }

}
