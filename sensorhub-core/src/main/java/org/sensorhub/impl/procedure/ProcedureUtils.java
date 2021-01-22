/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import java.time.Instant;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.vast.data.TextEncodingImpl;
import org.vast.ogc.gml.GMLUtils;
import org.vast.util.TimeExtent;
import com.rits.cloning.Cloner;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.IOPropertyList;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataStream;


public class ProcedureUtils
{
    
    public static AbstractProcess defaultToValidFromNow(AbstractProcess sml)
    {
        if (sml.getValidTime() == null)
        {
            var timeExtent = TimeExtent.endNow(Instant.now());
            var gmlTime = GMLUtils.timeExtentToTimePrimitive(timeExtent, true);
            sml.getValidTimeList().add(gmlTime);
        }
        
        return sml;
    }
    
    
    public static IOPropertyList extractOutputs(AbstractProcess sml)
    {
        var outputList = new IOPropertyList();
        sml.getOutputList().copyTo(outputList);
        sml.getOutputList().clear();
        return outputList;
    }
    
    
    public static void addDatastreamsFromOutputs(ProcedureTransactionHandler procHandler, IOPropertyList outputs) throws DataStoreException
    {
        for (var output: outputs)
        {
            if (output instanceof DataStream)
            {
                var ds = (DataStream)output;
                procHandler.addOrUpdateDataStream(ds.getName(), ds.getElementType(), ds.getEncoding());
            }
            else if (output instanceof DataComponent)
            {
                var comp = (DataComponent)output;
                procHandler.addOrUpdateDataStream(comp.getName(), comp, new TextEncodingImpl());
            }
        }
    }
    
    
    public static AbstractProcess addOutputsFromDatastreams(long procID, AbstractProcess sml, IDataStreamStore dataStreamStore)
    {
        var smlWithOutputs = new Cloner().deepClone(sml);
                
        dataStreamStore.select(new DataStreamFilter.Builder()
            .withProcedures(procID)
            .withCurrentVersion()
            .build())
        .forEach(ds -> {
            var output = ds.getRecordStructure();
            smlWithOutputs.getOutputList().add(ds.getOutputName(), output);
        });
        
        return smlWithOutputs;
    }
}
