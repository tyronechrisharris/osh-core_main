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

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Stream;
import org.sensorhub.api.procedure.IProcedureDescriptionStore;
import com.google.common.collect.Range;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Interface regrouping all data stores associated to a given procedure or
 * procedure group, and containing its historical data, including:
 * <li>observation time series</li>
 * <li>observed features of interest</li>
 * <li>procedure(s) description history</li>
 * </p>
 *
 * @author Alex Robin
 * @date Apr 2, 2018
 */
public interface IProcedureHistoricalData
{
    public static class ObsSeriesInfo
    {
        /**
         * Series name
         */
        String name;
        
        /**
         * Record schema version
         */
        int version;
        
        /**
         * Result time range during which the record schema version was in use
         */
        Range<Instant> resultTimeRange;
    }
    
    
    /**
     * @return Unique IDs of all leaf procedures whose data is persisted in this
     * storage unit
     */
    public Stream<String> getProcedureUIDs();
    
    
    /**
     * @return Unique IDs of all procedure groups whose data is persisted in
     * this storage unit
     */
    public Stream<String> getProcedureGroupUIDs();
    
    
    /**
     * @return Data store containing history of procedure descriptions
     */
    public IProcedureDescriptionStore getDescriptionHistoryStore();
    
    
    /**
     * @return Data store containing all features of interests observed by
     * this procedure
     */
    public IFoiStore getFeatureOfInterestStore();
    
    
    /**
     * @return Collection containing info about all observation series
     * available from this procedure
     */
    public Collection<ObsSeriesInfo> getObservationSeries();
    
    
    /**
     * @param seriesName name of observation series
     * @param version record schema version (to support schema changes)
     * @return Observation store containing data for the given series
     */
    public IObsStore getObservationStore(String seriesName, int version);
    
    
    /**
     * Adds a new observation time series type in this data store
     * @param name name of new observation store (should be unique and match
     *        data source output name)
     * @param recordStruct SWE data component describing the record structure
     * @param encoding recommended encoding for this record type
     * @return Newly created data store
     */
    public IObsStore addObsStore(String name, DataComponent recordStruct, DataEncoding encoding);
    
}
