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

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.time.Instant;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.api.procedure.IProcedureWithState;
import org.sensorhub.api.procedure.IProcedureGroup;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.vast.util.Asserts;
import com.google.common.collect.Range;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Proxy class reflecting the latest state of the attached live procedure.<br/>
 * This class tries to get data from the live procedure if available
 * and falls back on retrieving info from its own cache (which may be 
 * persistent) otherwise.<br/>
 * It is also serializable so that cached info can be persisted to the
 * procedure registry data store.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 5, 2019
 */
class ProcedureProxy implements IProcedureWithState, Serializable, IEventListener
{
    private static final long serialVersionUID = 978629974573336266L;
    public static final String NO_LISTENER_MSG = "Cannot register listener on a proxy class. Use the event bus";
    
    transient IProcedureRegistry registry;
    transient WeakReference<IProcedureWithState> ref; // reference to live procedure
    transient IEventPublisher eventPublisher;
        
    IEventSourceInfo eventSrcInfo;
    long lastDescriptionUpdate = Long.MIN_VALUE;
    AbstractProcess lastDescription;
    String parentUID;
        
    
    // needed for efficient deserialization
    protected ProcedureProxy()
    {
    }
    
    
    public ProcedureProxy(IProcedureWithState liveProcedure, IProcedureRegistry registry)
    {
        Asserts.checkNotNull(liveProcedure, IProcedureWithState.class);
        
        setProcedureRegistry(registry);
        connectLiveProcedure(liveProcedure);
    }
    
    
    /*
     * Used when deserializing from data store
     */
    public void setProcedureRegistry(IProcedureRegistry registry)
    {
        Asserts.checkNotNull(registry, IProcedureRegistry.class);
        this.registry = registry;
    }
    
    
    public void connectLiveProcedure(IProcedureWithState proc)
    {
        this.ref = new WeakReference<>(proc);
        captureState();
        
        // listen to all procedure events
        eventSrcInfo = proc.getEventSourceInfo();
        eventPublisher = registry.getParentHub().getEventBus().getPublisher(eventSrcInfo);
        proc.registerListener(this);
    }


    @Override
    public void handleEvent(Event e)
    {
        if (e instanceof ProcedureEvent) // need this check to filter out module events
        {
            boolean changed = captureState();
            
            // forward procedure events to bus
            eventPublisher.publish(e);
            
            if (e instanceof ProcedureChangedEvent)
                updateInDatastore(changed);
            else if (e instanceof ProcedureRemovedEvent)
                disconnectLiveProcedure((IProcedureWithState)e.getSource());
        }
    }
    
    
    public void disconnectLiveProcedure(IProcedureWithState proc)
    {
        proc.unregisterListener(this);
        ref.clear();
    }
    
    
    protected FeatureKey updateInDatastore(boolean changed)
    {
        Asserts.checkNotNull(lastDescription, "lastDescription");
        
        // TODO compute and compare description hashes to check if there 
        // was really an update and manage valid time accordingly
        
        return registry.addVersion(this);
    }
    
    
    protected FeatureKey createInDataStore()
    {
        return registry.add(this);
    }
    
    
    /*
     * Capture procedure state and save it in proxy
     */
    public boolean captureState()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null)
            return captureState(proc);
        return false;
    }
    
    
    protected boolean captureState(IProcedureWithState proc)
    {
        long previousUpdate = lastDescriptionUpdate;
        
        lastDescriptionUpdate = proc.getLastDescriptionUpdate();
        lastDescription = proc.getCurrentDescription();
        parentUID = proc.getParentGroup() != null ? proc.getParentGroup().getUniqueIdentifier() : null;
        
        return previousUpdate < lastDescriptionUpdate;
    }


    @Override
    public String getUniqueIdentifier()
    {
        return getCurrentDescription().getUniqueIdentifier();
    }
    
    
    @Override
    public String getName()
    {
        return getCurrentDescription().getName();
    }


    @Override
    public String getDescription()
    {
        return getCurrentDescription().getDescription();
    }


    @Override
    public Range<Instant> getValidTime()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null)
            return proc.getValidTime();
        else if (lastDescription != null)
            return lastDescription.getValidTime();
        else
            return Range.atLeast(Instant.ofEpochMilli(lastDescriptionUpdate));
    }


    @Override
    public IEventSourceInfo getEventSourceInfo()
    {
        return eventSrcInfo;
    }


    @Override
    public AbstractProcess getCurrentDescription()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null)
            return proc.getCurrentDescription();
        else
            return lastDescription;
    }


    @Override
    public IProcedureGroup<? extends IProcedureWithState> getParentGroup()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null)
            return proc.getParentGroup();
        else if (parentUID != null)
            return (IProcedureGroup<?>)registry.get(parentUID);
        else
            return null;
    }


    @Override
    public long getLastDescriptionUpdate()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null)
            return proc.getLastDescriptionUpdate();
        else
            return lastDescriptionUpdate;
    }


    @Override
    public boolean isEnabled()
    {
        IProcedureWithState liveProc = ref.get();
        return liveProc != null && liveProc.isEnabled();
    }


    @Override
    public void registerListener(IEventListener listener)
    {
        throw new UnsupportedOperationException(NO_LISTENER_MSG);
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
    }

}
