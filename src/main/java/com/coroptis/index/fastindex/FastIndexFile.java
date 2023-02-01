package com.coroptis.index.fastindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.DataFileIterator;
import com.coroptis.index.Pair;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.type.TypeDescriptor;
import com.coroptis.index.type.TypeDescriptorInteger;

/**
 * Provide information about keys and particular index files.
 * 
 * @author honza
 *
 * @param <K>
 */
public class FastIndexFile<K> implements CloseableResource {

    private final static String FILE_NAME = "index.map";

    private List<Pair<K, Integer>> list = new ArrayList<>();
    private final SortedDataFile<K, Integer> sdf;
    private final Comparator<K> keyComparator;

    FastIndexFile(final Directory directory, final TypeDescriptor<K> keyTypeDescriptor) {
	final TypeDescriptorInteger itd = new TypeDescriptorInteger();
	this.keyComparator = Objects.requireNonNull(keyTypeDescriptor.getComparator());
	this.sdf = new SortedDataFile<>(directory, FILE_NAME, itd.getTypeWriter(), itd.getTypeReader(),
		keyTypeDescriptor.getComparator(), keyTypeDescriptor.getConvertorFromBytes(),
		keyTypeDescriptor.getConvertorToBytes());
	try (final DataFileIterator<K, Integer> reader = sdf.openIterator()) {
	    while (reader.hasNext()) {
		final Pair<K, Integer> pair = reader.next();
		list.add(pair);
	    }
	}
    }

    public Integer findFileId(final K key) {
	Objects.requireNonNull(key, "Key can't be null");
	for (final Pair<K, Integer> pair : list) {
	    if (keyComparator.compare(key, pair.getKey()) <= 0) {
		return pair.getValue();
	    }
	}
	return null;
    }

    public int insertKeyToPage(final K key) {
	Objects.requireNonNull(key, "Key can't be null");
	for (final Pair<K, Integer> pair : list) {
	    if (keyComparator.compare(key, pair.getKey()) <= 0) {
		return pair.getValue();
	    }
	}
	/*
	 * Key is bigger that all key so it will at last segment. But key at last
	 * segment is smaller than adding one. Because of that key have to be upgraded.
	 */
	return updateMaxKey(key);
    }

    private int updateMaxKey(final K key) {
	if (list.size() == 0) {
	    list.add(Pair.of(key, 0));
	    return 0;
	} else {
	    final int lastIndex = list.size() - 1;
	    final Pair<K, Integer> max = list.get(lastIndex);
	    final Pair<K, Integer> newMax = Pair.of(key, max.getValue());
	    list.set(lastIndex, newMax);
	    return newMax.getValue();
	}
    }

    public void insertPage(final K key, final Integer pageId) {
	Objects.requireNonNull(key, "Key can't be null");
	int index = getIndexForKey(key);
	if (index == list.size()) {
	    list.add(Pair.of(key, pageId));
	} else {
	    list.add(index, Pair.of(key, pageId));
	}
    }

    public Stream<Pair<K, Integer>> getPagesAsStream() {
	return list.stream();
    }

    private int getIndexForKey(final K key) {
	int i = 0;
	for (final Pair<K, Integer> pair : list) {
	    if (keyComparator.compare(key, pair.getKey()) < 0) {
		return i;
	    }
	    i++;
	}
	return list.size();
    }

    public void save() {
	try (final SortedDataFileWriter<K, Integer> writer = sdf.openWriter()) {
	    list.forEach(writer::put);
	}
    }

    @Override
    public void close() {
	save();
    }

}
