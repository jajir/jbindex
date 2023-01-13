package com.coroptis.index;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.StreamSupport;

import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;

public class IndexMerge<K, V> {

    private final Directory directory1;
    private final Directory directory2;
    private final Directory output;
    private final ValueMerger<K, V> merger;
    private final Class<?> keyClass;
    private final Class<?> valueClass;
    final Comparator<? super K> keyComparator;
    private final int blockSize;

    public IndexMerge(final Directory directory1, final Directory directory2, final Directory output,
	    final ValueMerger<K, V> merger, final Class<?> keyClass, final Class<?> valueClass,
	    final int blockSize) {
	this.directory1 = Objects.requireNonNull(directory1);
	this.directory2 = Objects.requireNonNull(directory2);
	this.output = Objects.requireNonNull(output);
	this.merger = Objects.requireNonNull(merger);
	this.keyClass = Objects.requireNonNull(keyClass);
	this.valueClass = Objects.requireNonNull(valueClass);
	this.blockSize = Objects.requireNonNull(blockSize);
	final TypeConvertors tc = TypeConvertors.getInstance();
	this.keyComparator = tc.get(keyClass, OperationType.COMPARATOR);

    }

    public void merge() {
	final BasicIndex<K, V> basicIndex1 = new BasicIndex<>(directory1, keyClass, valueClass);
	final BasicIndex<K, V> basicIndex2 = new BasicIndex<>(directory1, keyClass, valueClass);
	try (final IndexIterator<K, V> iterator1 = new IndexSearcher<K, V>(directory1, keyClass,
		valueClass, basicIndex1).getIterator()) {
	    try (final IndexIterator<K, V> iterator2 = new IndexSearcher<K, V>(directory2, keyClass,
		    valueClass, basicIndex2).getIterator()) {
		final IndexReader<K, V> indexReader1 = new IndexReader<>(iterator1);
		final IndexReader<K, V> indexReader2 = new IndexReader<>(iterator2);

		try (final IndexWriter<K, V> out = new IndexWriter<>(output, blockSize, keyClass,
			valueClass)) {
		    StreamSupport
			    .stream(new MergeIndexSpliterator<K, V>(indexReader1, indexReader2,
				    keyComparator, merger), false)
			    .forEach(pair -> out.put(pair.getKey(), pair.getValue()));
		}
	    }
	}
    }

}
