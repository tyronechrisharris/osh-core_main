/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.namespace.QName;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVStore;
import org.sensorhub.api.datastore.feature.FeatureFilter;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.TimeExtent;
import net.opengis.gml.v32.AbstractGeometry;


public class MVFeatureStoreImpl extends MVBaseFeatureStoreImpl<IFeature, FeatureField, FeatureFilter> implements IFeatureStore
{
    interface IGeoTemporalFeature extends IGeoFeature, ITemporalFeature {}
    
    
    protected MVFeatureStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing feature store or create a new one with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param newStoreInfo Data store info to use if a new store needs to be created
     * @return The existing datastore instance 
     */
    public static MVFeatureStoreImpl open(MVStore mvStore, MVDataStoreInfo newStoreInfo)
    {
        var dataStoreInfo = H2Utils.getDataStoreInfo(mvStore, newStoreInfo.getName());
        if (dataStoreInfo == null)
        {
            dataStoreInfo = newStoreInfo;
            H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        }
        
        return (MVFeatureStoreImpl)new MVFeatureStoreImpl().init(mvStore, dataStoreInfo, null);
    }
    
    
    @Override
    protected Stream<Entry<MVFeatureParentKey, IFeature>> getIndexedStream(FeatureFilter filter)
    {
        var resultStream = super.getIndexedStream(filter);
        
        if (filter.getParentFilter() != null)
        {
            if (resultStream == null)
            {
                resultStream = selectParentIDs(filter.getParentFilter())
                    .flatMap(id -> getParentResultStream(id, filter.getValidTime()));
            }
            else
            {
                var parentIDs = selectParentIDs(filter.getParentFilter())
                    .collect(Collectors.toSet());
                
                resultStream = resultStream.filter(
                    e -> parentIDs.contains(((MVFeatureParentKey)e.getKey()).getParentID()));
                
                // post filter using keys valid time if needed
                if (filter.getValidTime() != null)
                    resultStream = postFilterKeyValidTime(resultStream, filter.getValidTime());
            }
        }
        
        return resultStream;
    }
    
    
    protected Stream<Long> selectParentIDs(FeatureFilter parentFilter)
    {
        return DataStoreUtils.selectFeatureIDs(this, parentFilter);
    }


    @Override
    public Stream<Entry<FeatureKey, IFeature>> selectEntries(FeatureFilter filter, Set<FeatureField> fields)
    {
        // update validTime in the case it ends at now and there is a
        // more recent version of the procedure description available
        Stream<Entry<FeatureKey, IFeature>> resultStream = super.selectEntries(filter, fields).map(e -> {
            if (e.getValue() instanceof ITemporalFeature)
            {
                var f = (ITemporalFeature)e.getValue();
                var fWrap = new IGeoTemporalFeature()
                {
                    public String getId() { return f.getId(); }
                    public String getUniqueIdentifier() { return f.getUniqueIdentifier(); }
                    public String getName() { return f.getName(); }
                    public String getDescription() { return f.getDescription(); }
                    public Map<QName, Object> getProperties() { return f.getProperties(); }
                    
                    public AbstractGeometry getGeometry()
                    {
                        return f instanceof IGeoFeature ?
                            ((IGeoFeature)f).getGeometry() : null;
                    }
                    
                    public TimeExtent getValidTime()
                    {
                        var nextKey = featuresIndex.higherKey((MVFeatureParentKey)e.getKey());
                        if (nextKey != null && nextKey.getInternalID() == e.getKey().getInternalID() &&
                            f.getValidTime() != null && f.getValidTime().endsNow())
                            return TimeExtent.period(f.getValidTime().begin(), nextKey.getValidStartTime());
                        else
                            return f.getValidTime();
                    }
                };
                
                return new DataUtils.MapEntry<FeatureKey, IFeature>(e.getKey(), fWrap);
            }
            else
                return e;
        });
        
        // apply post filter on time now that we computed the correct valid time period
        if (filter.getValidTime() != null)
            resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        
        return resultStream;
    }

}
