package com.coroptis.index.simpledatafile;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairFileWriter;
import com.coroptis.index.directory.Props;

/**
 * Decorator of PairWriter that count how many key value pairs was written.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class PairWriterCountPair<K, V> implements PairFileWriter<K, V> {

    private final PairFileWriter<K, V> pairWriter;
    private final Props props;
    final static String NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE = "number_of_key_value_pairs_in_cache";

    PairWriterCountPair(final PairFileWriter<K, V> pairWriter, final Props props) {
        this.pairWriter = Objects.requireNonNull(pairWriter);
        this.props = Objects.requireNonNull(props);
    }

    @Override
    public void close() {
        pairWriter.close();
        props.writeData();
    }

    @Override
    public void put(Pair<K, V> pair) {
        pairWriter.put(pair);
        long counter = props.getLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE);
        counter++;
        props.setLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE, counter);
    }

}
