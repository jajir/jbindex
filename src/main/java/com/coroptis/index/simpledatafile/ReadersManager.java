package com.coroptis.index.simpledatafile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ReadersManager<K, V> {

    private List<SstPairReader<K, V>> readers = new ArrayList<>();

    public void register(final SstPairReader<K, V> reader) {
        Objects.requireNonNull(reader);
        readers.add(reader);
        reader.setOnCloseConsumer(sstReader -> {
            readers.remove(sstReader);
        });
    }

    void makeDirty() {
        readers.forEach(reader -> reader.makeDirty());
    }

}
