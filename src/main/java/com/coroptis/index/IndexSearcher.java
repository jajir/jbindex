package com.coroptis.index;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import com.coroptis.index.simpleindex.CloseableResource;
import com.coroptis.index.simpleindex.Pair;
import com.coroptis.index.simpleindex.SimpleIndexReader;
import com.coroptis.index.storage.Directory;
import com.coroptis.index.type.IntegerTypeDescriptor;
import com.coroptis.index.type.TypeRawArrayReader;
import com.coroptis.index.type.TypeStreamReader;

public class IndexSearcher<K, V> implements CloseableResource {

    private final ArrayList<Pair<K, Integer>> metaIndex = new ArrayList<>();

    private final Directory directory;

    private final TypeRawArrayReader<K> keyTypeRawArrayReader;

    private final IntegerTypeDescriptor integerTypeDescriptor = new IntegerTypeDescriptor();

    private final SimpleIndexReader<K, V> mainIndexReader;

    private final Comparator<? super K> keyComparator;

    private final IndexDesc indexDesc;

    public IndexSearcher(final Directory directory, final TypeRawArrayReader<K> keyTypeRawArrayReader,
	    final Comparator<? super K> keyComparator, final TypeStreamReader<V> valueTypeStreamReader) {
	this.directory = Objects.requireNonNull(directory, "directory must not be null");
	this.keyTypeRawArrayReader = Objects.requireNonNull(keyTypeRawArrayReader,
		"keyTypeRawArrayReader must not be null");
	this.indexDesc = IndexDesc.load(directory);
	this.keyComparator = Objects.requireNonNull(keyComparator, "keyComparator must not be null");
	this.mainIndexReader = new SimpleIndexReader<>(directory.getFileReader(IndexWriter.INDEX_MAIN_DATA_FILE),
		keyTypeRawArrayReader, valueTypeStreamReader, keyComparator);
	loadMetaIndex();
    }

    private void loadMetaIndex() {
	try (final SimpleIndexReader<K, Integer> sir = new SimpleIndexReader<>(
		directory.getFileReader(IndexWriter.INDEX_META_FILE), keyTypeRawArrayReader,
		integerTypeDescriptor.getStreamReader(), keyComparator)) {
	    sir.stream(indexDesc.getWrittenBlockCount()).forEach(pair -> metaIndex.add(pair));
	}
    }

    public V get(final K key) {
	Objects.requireNonNull(key, "key must not be null");
	Integer blockId = getBlock(key);
	if (blockId >= 0) {
	    mainIndexReader.seek(blockId);
	    final Optional<Pair<K, V>> oVal = mainIndexReader.stream(indexDesc.getWrittenKeyCount())
		    .limit(indexDesc.getBlockSize()).filter(pair -> keyComparator.compare(pair.getKey(), key) == 0)
		    .findFirst();
	    if (oVal.isPresent()) {
		return oVal.get().getValue();
	    }
	}
	return null;
    }

    private Integer getBlock(final K key) {
	Pair<K, Integer> previousPair = null;
	for (final Pair<K, Integer> blockPair : metaIndex) {
	    if (keyComparator.compare(blockPair.getKey(), key) > 0) {
		/**
		 * key doesn't belongs to this block. So it belong to previous one.
		 */
		if (previousPair == null) {
		    return -2; // no such block
		} else {
		    return previousPair.getValue();
		}
	    }
	    previousPair = blockPair;
	}
	return previousPair.getValue();
    }

    public Stream<Pair<K, V>> stream() {
	return mainIndexReader.stream(indexDesc.getWrittenKeyCount());
    }

    @Override
    public void close() {
	mainIndexReader.close();
    }

}
