package com.coroptis.index.segment;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.scarceindex.ScarceIndexWriter;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class Segment<K, V> implements CloseableResource {

    private final static String INDEX_FILE_NAME_EXTENSION = File.separator
            + "index";
    private final static String SCARCE_FILE_NAME_EXTENSION = File.separator
            + "scarce";
    private final static String CACHE_FILE_NAME_EXTENSION = File.separator
            + "cache";
    private final static String TEMP_FILE_NAME_EXTENSION = File.separator
            + "tmp";
    private final Directory directory;
    private final SegmentId id;
    private final long maxNumeberOfKeysInSegmentCache;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;
    private final ScarceIndex<K> scarceIndex;
    private final SegmentStatsManager segmentStatsManager;
    private final static long SCARCE_INDEX_PAGE_SIZE = 1000;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final Directory directory, final SegmentId id,
            final long maxNumeberOfKeysInSegmentCache,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.maxNumeberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.cache = loadCache();
        this.scarceIndex = ScarceIndex.<K>builder().withDirectory(directory)
                .withFileName(getScarceFileName())
                .withKeyTypeDescriptor(keyTypeDescriptor).build();
        this.segmentStatsManager = new SegmentStatsManager(directory, id);
    }

    private UniqueCache<K, V> loadCache() {
        final SstFile<K, V> sstFile = getCacheSstFile();
        final UniqueCache<K, V> out = new UniqueCache<>(
                keyTypeDescriptor.getComparator());
        sstFile.openStreamer().stream().forEach(pair -> out.put(pair));
        return out;
    }

    private long getScarceIndexPageSize() {
        return SCARCE_INDEX_PAGE_SIZE;
    }

    private String getCacheFileName() {
        return id.getName() + CACHE_FILE_NAME_EXTENSION;
    }

    private String getTempIndexFileName() {
        return id.getName() + TEMP_FILE_NAME_EXTENSION
                + INDEX_FILE_NAME_EXTENSION;
    }

    private String getTempScarceFileName() {
        return id.getName() + TEMP_FILE_NAME_EXTENSION
                + SCARCE_FILE_NAME_EXTENSION;
    }

    private String getScarceFileName() {
        return id.getName() + SCARCE_FILE_NAME_EXTENSION;
    }

    private String getIndexFileName() {
        return id.getName() + INDEX_FILE_NAME_EXTENSION;
    }

    public SegmentStats getStats() {
        return segmentStatsManager.getSegmentStats();
    }

    private SstFile<K, V> getCacheSstFile() {
        return new SstFile<>(directory, getCacheFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    private SstFile<K, V> getIndexSstFile() {
        return new SstFile<>(directory, getIndexFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    private SstFile<K, V> getTempIndexFile() {
        return new SstFile<>(directory, getTempIndexFileName(),
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    UniqueCache<K, V> getCache() {
        return cache;
    }

    void flushCache() {
        if (cache.size() > maxNumeberOfKeysInSegmentCache) {
            forceCompact();
        } else {
            final AtomicLong cx = new AtomicLong(0);
            try (final SstFileWriter<K, V> writer = getCacheSstFile()
                    .openWriter()) {
                cache.getStream().forEach(pair -> {
                    writer.put(pair);
                    cx.incrementAndGet();
                });
            }
            segmentStatsManager.setNumberOfKeysInCache(cx.get());
        }
    }

    void optionallyCompact() {
        if (cache.size() > maxNumeberOfKeysInSegmentCache) {
            forceCompact();
        }
    }

    public Stream<Pair<K, V>> getStream() {
        return StreamSupport
                .stream(new MergeSpliterator<>(getIndexSstFile().openIterator(),
                        getCache().getSortedIterator(), keyTypeDescriptor,
                        valueTypeDescriptor), false);
    }

    void forceCompact() {
        final ScarceIndex<K> scarceTmp = ScarceIndex.<K>builder()
                .withDirectory(directory).withFileName(getTempScarceFileName())
                .withKeyTypeDescriptor(keyTypeDescriptor).build();
        final AtomicLong cx = new AtomicLong(0L);
        try (final ScarceIndexWriter<K> scarceWriter = scarceTmp.openWriter()) {
            try (final SstFileWriter<K, V> indexWriter = getTempIndexFile()
                    .openWriter()) {
                getStream().forEach(pair -> {
                    final long i = cx.getAndIncrement();
                    if (i % getScarceIndexPageSize() == 0) {
                        final int position = indexWriter.put(pair, true);
                        scarceWriter.put(Pair.of(pair.getKey(), position));
                    } else {
                        indexWriter.put(pair);
                    }
                });
            }
        }
        cache.clear();
        flushCache();
        segmentStatsManager.setNumberOfKeysInCache(0);
        segmentStatsManager.setNumberOfKeysInIndex(cx.get());
        segmentStatsManager.flush();
        directory.renameFile(getTempIndexFileName(), getIndexFileName());
        directory.renameFile(getTempScarceFileName(), getScarceFileName());
        scarceIndex.loadCache();
    }

    public SegmentWriter<K, V> openWriter() {
        return new SegmentWriter<>(this);
    }

    public V get(final K key) {
        final V out = cache.get(key);
        if (valueTypeDescriptor.isTombstone(out)) {
            return null;
        }
        if (out == null) {
            final Integer position = scarceIndex.get(key);
            if (position == null) {
                return null;
            }
            return getIndexSstFile().openStreamerFromPosition(position).stream()
                    .filter(pair -> pair.getKey().equals(key)).findAny()
                    .map(pair -> pair.getValue()).orElseGet(() -> null);
        }
        return out;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

}
