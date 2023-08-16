package com.coroptis.index.sst;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.PairIterator;
import com.coroptis.index.PairReader;
import com.coroptis.index.PairReaderEmpty;
import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class SortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<V> valueReader;

    private final Comparator<K> keyComparator;

    private final ConvertorFromBytes<K> keyConvertorFromBytes;

    private final ConvertorToBytes<K> keyConvertorToBytes;

    public static <M, N> SortedDataFileBuilder<M, N> builder() {
        return new SortedDataFileBuilder<M, N>();
    }

    public SortedDataFile(final Directory directory, final String fileName,
            final TypeWriter<V> valueWriter, final TypeReader<V> valueReader,
            final Comparator<K> keyComparator,
            final ConvertorFromBytes<K> keyConvertorFromBytes,
            final ConvertorToBytes<K> keyConvertorToBytes) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        this.valueReader = Objects.requireNonNull(valueReader);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.keyConvertorFromBytes = Objects
                .requireNonNull(keyConvertorFromBytes);
        this.keyConvertorToBytes = Objects.requireNonNull(keyConvertorToBytes);
    }

    public SortedDataFileStreamer<K, V> openStreamer() {
        final SortedDataFileStreamer<K, V> streamer = new SortedDataFileStreamer<>(
                openReader(), keyComparator);
        return streamer;
    }

    public PairReader<K, V> openReader() {
        if (!directory.isFileExists(fileName)) {
            return new PairReaderEmpty<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(
                keyConvertorFromBytes);
        final SortedDataFileReader<K, V> reader = new SortedDataFileReader<>(
                diffKeyReader, valueReader, directory.getFileReader(fileName));
        return reader;
    }

    @SuppressWarnings("resource")
    public PairIterator<K, V> openIterator() {
        final PairIterator<K, V> iterator = new PairIterator<>(openReader());
        return iterator;
    }

    public SortedDataFileWriter<K, V> openWriter() {
        final SortedDataFileWriter<K, V> writer = new SortedDataFileWriter<>(
                directory, fileName, keyConvertorToBytes, keyComparator,
                valueWriter);
        return writer;
    }

}
