/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.stream.Stream;


/**
 * <p>
 * Base interface for all object data stores. This is an extension of the
 * {@link Map} interface that adds support for:
 * <li>Selecting, removing and counting elements matching a filter query</li>
 * <li>Backup/restore operations</li>
 * </p><p>
 * Many operations return {@link Stream} objects allowing additional
 * filtering, projecting and sorting using Java Stream API methods.
 * </p><p>
 * <i>Note that certain data store implementations may optimize execution
 * by partitioning, parallelizing or distributing the Stream pipeline
 * operations.</i>
 * </p></p> 
 * TODO: add async methods returning RxJava Flowables instead of Streams
 * @param <K> Key type
 * @param <V> Value type 
 * @param <Q> Query type
 **/
public interface IDataStore<K, V, Q extends IQueryFilter> extends Map<K, V>
{

    /**
     * @return Data store name
     */
    public String getDatastoreName();


    /**
     * @return Time zone to be used with all time stamps contained in this
     * data store
     */
    public ZoneOffset getTimeZone();


    /**
     * @return Total number of records contained in this data store
     */
    public long getNumRecords();


    /**
     * Select all entries matching the query and return values only
     * @param query selection filter (datastore specific)
     * @return Stream of value objects
     */
    public Stream<V> select(Q query);


    /**
     * Select all entries matching the query and return keys only
     * @param query selection filter (datastore specific)
     * @return Stream of key objects
     */
    public Stream<K> selectKeys(Q query);


    /**
     * Select all entries matching the query and return full entries
     * @param query selection filter (datastore specific)
     * @return Stream of matching entries (i.e. key/value pairs)
     */
    public Stream<Entry<K, V>> selectEntries(Q query);


    /**
     * Remove all entries matching the query
     * @param query selection filter (datastore specific)
     * @return Stream of deleted keys
     */
    public Stream<K> removeEntries(Q query);


    /**
     * Count all entries matching the query
     * @param query selection filter (datastore specific)
     * @return number of matching entries, or the value of query limit if
     * reached
     */
    public long countMatchingEntries(Q query);


    /**
     * Force flushing of cached data with underlying storage medium
     * @return true if sync was successful
     */
    public boolean sync();


    /**
     * Backup datastore content to the specified output stream
     * @param is target output stream
     * @throws IOException if backup failed
     */
    public void backup(OutputStream is) throws IOException;


    /**
     * Restore datastore content from the specified input stream
     * @param os source input stream
     * @throws IOException if restoration failed
     */
    public void restore(InputStream os) throws IOException;


    /**
     * @return true if reading from this datastore is supported
     */
    public boolean isReadSupported();


    /**
     * @return true if writing to this datastore is supported,
     * false if read-only
     */
    public boolean isWriteSupported();

}
