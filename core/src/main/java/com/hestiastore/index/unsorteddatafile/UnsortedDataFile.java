package com.hestiastore.index.unsorteddatafile;

import java.util.Objects;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.PairIteratorFromReader;
import com.hestiastore.index.PairWriter;
import com.hestiastore.index.datatype.TypeReader;
import com.hestiastore.index.datatype.TypeWriter;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.FileReader;
import com.hestiastore.index.directory.Directory.Access;

/**
 * Unsorted key value pairs storage file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public class UnsortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final TypeWriter<K> keyWriter;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<K> keyReader;

    private final TypeReader<V> valueReader;

    private final int diskIoBufferSize;

    public static <M, N> UnsortedDataFileBuilder<M, N> builder() {
        return new UnsortedDataFileBuilder<M, N>();
    }

    public UnsortedDataFile(final Directory directory, final String fileName,
            final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter,
            final TypeReader<K> keyReader, final TypeReader<V> valueReader,
            final int diskIoBufferSize) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.keyWriter = Objects.requireNonNull(keyWriter);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        this.keyReader = Objects.requireNonNull(keyReader);
        this.valueReader = Objects.requireNonNull(valueReader);
        this.diskIoBufferSize = diskIoBufferSize;
    }

    public PairIterator<K, V> openIterator() {
        final PairIterator<K, V> iterator = new PairIteratorFromReader<>(
                openReader());
        return iterator;
    }

    public PairWriter<K, V> openWriter() {
        return openWriter(Access.OVERWRITE);
    }

    public PairWriter<K, V> openWriter(final Access access) {
        Objects.requireNonNull(access);
        Access used = null;
        if (directory.isFileExists(fileName)) {
            used = access;
        } else {
            used = Access.OVERWRITE;
        }
        final UnsortedDataFileWriter<K, V> writer = new UnsortedDataFileWriter<>(
                directory, fileName, keyWriter, valueWriter, used,
                diskIoBufferSize);
        return writer;
    }

    public UnsortedDataFileStreamer<K, V> openStreamer() {
        if (directory.isFileExists(fileName)) {
            final UnsortedDataFileSpliterator<K, V> spliterator = new UnsortedDataFileSpliterator<>(
                    openReader());
            return new UnsortedDataFileStreamer<>(spliterator);
        } else {
            return new UnsortedDataFileStreamer<>(null);
        }
    }

    public CloseablePairReader<K, V> openReader() {
        final UnsortedDataFileReader<K, V> out = new UnsortedDataFileReader<>(
                keyReader, valueReader, getFileReader());
        return out;
    }

    private FileReader getFileReader() {
        return directory.getFileReader(fileName, diskIoBufferSize);
    }

}
