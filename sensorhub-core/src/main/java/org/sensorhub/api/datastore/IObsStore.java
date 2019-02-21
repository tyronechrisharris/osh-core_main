/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;
import com.google.common.collect.Range;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Generic interface for observation stores.<br/>
 * Observations retrieved by select methods are sorted by phenomenon time and
 * grouped by result time when several result times are requested.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 19, 2018
 */
public interface IObsStore extends IDataStore<ObsKey, ObsData, ObsFilter>
{

    /**
     * @return Description of records contained in this data store
     */
    public DataComponent getRecordDescription();
    
            
    /**
     * @return Encoding method used for records contained in this data store
     */
    public DataEncoding getRecommendedEncoding();
    
    
    /**
     * @return Phenomenon time span of all observations contained in this
     * data store, or null if store is empty
     */
    public Range<Instant> getPhenomenonTimeRange();
    
    
    /**
     * @return Average/approximate period between consecutive observed
     * phenomenon occurrences (in seconds)
     */
    public Duration getPhenomenonTimeStep();
    
    
    /**
     * Gets a set of more precise time periods for which observations are
     * available in this data store, sorted by procedure and then by time
     * @param filter filtering criteria
     * @return Stream of observation periods
     */
    public Stream<ObsCluster> getObsClustersByPhenomenonTime(ObsClusterFilter filter);
    
    
    /**
     * @return Result time span of all observations contained in this data
     * store, or null if store is empty
     */
    public Range<Instant> getResultTimeRange();
    
    
    /**
     * @return Average/approximate period between consecutive generations of
     * observation results (in seconds)
     */
    public Duration getResultTimeStep();
    
    
    /**
     * Gets a set of more precise time periods during which results of
     * observations contained in this data store were generated, sorted by
     * procedure and then by time<br/>
     * In cases where an entire group of observations is generated at the exact
     * same time (e.g. model runs), a period can be collapsed to a single instant
     * @param filter filtering criteria
     * @return Stream of time periods 
     */
    public Stream<ObsCluster> getObsClustersByResultTime(ObsClusterFilter filter);
    
    
    /**
     * Select all observations matching the query and return result datablocks only
     * @param query selection filter (datastore specific)
     * @return Stream of result data blocks
     */
    public Stream<DataBlock> selectResults(ObsFilter query);
    
}
