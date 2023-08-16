/**
 * 
 */
/**
 * Package responsible for index segment. Segment is part of index structure supporting operations:
 * <ul>
 * <li>get(k) --> v</li>
 * <li>put(k,v)</li>
 * <li>delete(k)</li>
 * </ul>
 * 
 * Segment consist from following parts:
 * <ul>
 * <li>SST - Sorted String Table with unique key records</li>
 * <li>meta - some meta information about index like bigger key, number of recorst</li>
 * <li>unsorted - log containing chnages in segment. When this file grow up to some point than have to be merged into SST</li>
 * <li>sparse index - point do exact places in main SST file. It speed up read operation</li>
 * </ul>
 * 
 * 
 * @author jajir
 *
 */
package com.coroptis.index.segment;