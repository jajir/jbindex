package com.coroptis.index.sorteddatafile;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.FileReader;

import java.util.Objects;

public class SegmentReader {

        private final TypeDescriptor<Integer> TDI = new TypeDescriptorInteger();

        private final DataCompressor dataCompressor;

        private final FileReader reader;

        public SegmentReader(DataCompressor dataCompressor, final FileReader reader) {
                this.dataCompressor = Objects.requireNonNull(dataCompressor, "data compressor must not be null");
                this.reader = Objects.requireNonNull(reader, "reader must not be null");
        }

        public byte[] readNext() {
                int size = TDI.getTypeReader().read(reader);
                byte[] compressed = new byte[size];
                reader.read(compressed);
                return dataCompressor.deCompress(compressed);
        }
}
