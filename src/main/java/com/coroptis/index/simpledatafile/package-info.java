/**
 * 
 */
/**
 * Package contains simple index file.It consists from parts sorted data file
 * file with changes and map file.
 * 
 * Sorted data file is main place for data. It contains sorted data.
 * Disadvantage is that insert new key require rebuilding whole file. It slow
 * down inserting process.
 * 
 * Change file contains unsorted changes. It means newly added data or changes
 * in index. When new change comes it's added to this file. Disadvantage is that
 * this file have to be fully loaded before further work with whole index.
 * 
 * Map file I'm not going to implement.
 * 
 * @author jajir
 *
 */
package com.coroptis.index.simpledatafile;