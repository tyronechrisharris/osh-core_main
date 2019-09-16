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

import java.util.stream.Stream;
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
     * Select all observations matching the query and return result datablocks only
     * @param query selection filter (datastore specific)
     * @return Stream of result data blocks
     */
    public Stream<DataBlock> selectResults(ObsFilter query);
    
    
    /**
     * Select statistics for procedures and FOI matching the query
     * @param query filter to select desired procedures and FOIs
     * @return stream of statistics buckets. Each item represents statistics for
     * observations collected for a combination of procedure, feature of
     * interest, and result time. 
     */
    public Stream<ObsStats> selectStatistics(ObsStatsFilter query);
    
}
