package com.coroptis.index.sorteddatafile;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.TypeReader;

public class SortedDataFileReader<K, V>
        implements PairReader<K, V>, CloseableResource {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReader reader;

    SortedDataFileReader(final TypeReader<K> keyReader,
            final TypeReader<V> valueReader, final FileReader reader) {
        this.keyTypeReader = Objects.requireNonNull(keyReader);
        this.valueTypeReader = Objects.requireNonNull(valueReader);
        this.reader = Objects.requireNonNull(reader);
    }

    @Override
    public Pair<K, V> read() {
        final K key = keyTypeReader.read(reader);
        if (key == null) {
            return null;
        } else {
            final V value = valueTypeReader.read(reader);
            return new Pair<K, V>(key, value);
        }
    }

    @Override
    public void close() {
        reader.close();
    }

}
