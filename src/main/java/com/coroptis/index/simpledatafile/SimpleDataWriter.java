package com.coroptis.index.simpledatafile;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;

public class SimpleDataWriter<K,V> implements PairWriter<K, V>{
    
    /**
     * 
     * @param pair
     */
    public void put(final Pair<K, V> pair) {
        //Support adding of change without loading all changes
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

}
