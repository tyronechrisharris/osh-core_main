/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.util.stream.Stream;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Storage interface regrouping all data stores associated to a given
 * procedure or procedure group, including:
 * <li>observation time series</li>
 * <li>observed features of interest</li>
 * <li>procedure(s) description history</li>
 * </p>
 *
 * @author Alex Robin
 * @date Apr 2, 2018
 */
public interface IProcedureStorage
{
    
    /**
     * @return Unique ID of storage unit
     */
    public String getStorageUniqueID();
    
    
    /**
     * @return Unique ID of procedure associated with this storage unit.
     * In the case of a procedure group, this is the unique ID of the group
     * and IDs of group members can be retrieved with {@link #getGroupMemberIDs()}.
     */
    public String getProcedureUniqueID();
    
    
    /**
     * @return Unique IDs of all procedures that are member of the procedure 
     * group and that contributed data to this storage unit. Empty stream
     * if the associated procedure is not a group.
     */
    public Stream<String> getMemberProcedureIDs();
    
    
    /**
     * @return Data store containing history of procedure descriptions
     */
    public IProcedureStore getDescriptionHistoryStore();
    
    
    /**
     * @return Data store containing all features of interests observed by
     * this procedure
     */
    public IFoiStore getFeatureOfInterestStore();
    
    
    /**
     * @return Names of all observation series available in this storage unit
     */
    public Stream<String> getObservationSeriesNames();
    
    
    /**
     * @param seriesName name of observation series
     * @return Observation store containing data for the given series
     */
    public IObsStore getObservationStore(String seriesName);
    
    
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
