/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.vast.data.DataIterator;
import org.vast.ows.sos.SOSOfferingCapabilities;
import org.vast.ows.sos.SOSServiceCapabilities;
import org.vast.ows.swe.SWESOfferingCapabilities;
import org.vast.swe.SWEConstants;
import org.vast.util.TimeExtent;
import com.google.common.base.Strings;


public class CapabilitiesUpdater
{
    public static final String OFFERING_NAME_PLACEHOLDER = "{%offering_name%}";
    public static final String PROC_UID_PLACEHOLDER = "{%procedure_uid%}";
    public static final String PROC_NAME_PLACEHOLDER = "{%procedure_name%}";
    public static final String PROC_DESC_PLACEHOLDER = "{%procedure_description%}";
    
    SOSServlet servlet;
    
    
    public CapabilitiesUpdater(SOSServlet servlet)
    {
        this.servlet = servlet;
    }
    
    
    public void updateOfferings(final SOSServiceCapabilities caps)
    {
        Map<String, SOSOfferingCapabilities> offerings = new LinkedHashMap<>();
        
        var obsDb = servlet.readDatabase;
        var providerConfigs = servlet.providerConfigs;
        
        obsDb.getProcedureStore().selectEntries(new ProcedureFilter.Builder().build())
            .forEach(entry -> {
                var procID = entry.getKey().getInternalID();
                var proc = entry.getValue();
                
                String procUID = proc.getUniqueIdentifier();
                var customConfig = providerConfigs.get(procUID);
                
                // retrieve or create new offering
                SOSOfferingCapabilities offering = offerings.get(procUID);
                if (offering == null)
                {
                    offering = new SOSOfferingCapabilities();
                    offering.setIdentifier(procUID); // use procedure UID as offering ID
                    offering.getProcedures().add(procUID);
                    
                    // use name and description from custom config if set
                    // otherwise default to name and description of procedure 
                    offering.setTitle(customConfig != null && !Strings.isNullOrEmpty(customConfig.name) ?
                        replaceVariables(customConfig.name, proc, customConfig) : proc.getName());
                    offering.setDescription(customConfig != null && !Strings.isNullOrEmpty(customConfig.description) ?
                        replaceVariables(customConfig.description, proc, customConfig) : proc.getDescription());
                    
                    // add supported formats
                    offering.getResponseFormats().add(SWESOfferingCapabilities.FORMAT_OM2);
                    offering.getResponseFormats().add(SWESOfferingCapabilities.FORMAT_OM2_JSON);
                    offering.getProcedureFormats().add(SWESOfferingCapabilities.FORMAT_SML2);
                    offering.getProcedureFormats().add(SWESOfferingCapabilities.FORMAT_SML2_JSON);
                    
                    offerings.put(procUID, offering);
                }
                
                // process all procedure datastreams
                var finalOffering = offering;
                var dsFilter = new DataStreamFilter.Builder()
                    .withProcedures(procID)
                    .build();
                
                obsDb.getDataStreamStore().select(dsFilter)
                   .forEach(dsInfo -> {
                                              
                       // iterate through all SWE components and add all definition URIs as observables
                       // this way only composites with URI will get added
                       DataIterator it = new DataIterator(dsInfo.getRecordStructure());
                       while (it.hasNext())
                       {
                           String defUri = it.next().getDefinition();
                           if (defUri != null &&
                               !defUri.equals(SWEConstants.DEF_SAMPLING_TIME) &&
                               !defUri.equals(SWEConstants.DEF_PHENOMENON_TIME))
                               finalOffering.getObservableProperties().add(defUri);
                       }
                       
                       var timeRange = dsInfo.getPhenomenonTimeRange();
                       if (timeRange != null)
                       {
                           if (finalOffering.getPhenomenonTime() == null)
                               finalOffering.setPhenomenonTime(timeRange);
                           else
                               finalOffering.setPhenomenonTime(
                                   TimeExtent.span(finalOffering.getPhenomenonTime(), timeRange));
                       }
                   });
                                
                var phenTimeRange = offering.getPhenomenonTime();
                
                // set end to 'now' if timeout not reached yet
                var timeOut = servlet.config.defaultLiveTimeout;
                if (customConfig != null && customConfig instanceof ProcedureDataProviderConfig)
                    timeOut = ((ProcedureDataProviderConfig)customConfig).liveDataTimeout;
                
                if (phenTimeRange != null &&
                    phenTimeRange.end().isAfter(Instant.now().minusMillis((long)(timeOut*1000.))))
                    offering.setPhenomenonTime(TimeExtent.endNow(phenTimeRange.begin()));
            });
        
        caps.getLayers().clear();
        caps.getLayers().addAll(offerings.values());
    }
    
    
    protected String replaceVariables(String textField, IProcedureWithDesc proc, SOSProviderConfig config)
    {
        textField.replace(PROC_UID_PLACEHOLDER, proc.getUniqueIdentifier());
        
        if (config.name != null)
            textField = textField.replace(OFFERING_NAME_PLACEHOLDER, config.name);
        else if (proc.getName() != null)
            textField = textField.replace(OFFERING_NAME_PLACEHOLDER, proc.getName());
        
        if (proc.getName() != null)
            textField = textField.replace(PROC_NAME_PLACEHOLDER, proc.getName());
        
        if (proc.getDescription() != null)
            textField = textField.replace(PROC_DESC_PLACEHOLDER, proc.getDescription());
        
        return textField;
    }
}
