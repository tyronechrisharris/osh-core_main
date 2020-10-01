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
import java.util.Map;
import javax.xml.namespace.QName;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.api.procedure.IProcedureWithState;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.procedure.IProcedureGroup;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Wrapper class reflecting the latest state of the attached live procedure.<br/>
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
public class ProcedureShadow implements IProcedureWithState, Serializable, IEventListener
{
    private static final long serialVersionUID = 978629974573336266L;
    public static final String NO_LISTENER_MSG = "Cannot register listener on a proxy class. Use the event bus";

    protected transient DefaultProcedureRegistry registry;
    protected transient WeakReference<IProcedureWithState> ref; // reference to live procedure
    protected transient IEventPublisher eventPublisher;

    protected IEventSourceInfo eventSrcInfo;
    protected long lastDescriptionUpdate = Long.MIN_VALUE;
    protected AbstractProcess latestDescription;
    protected ProcedureId procedureId;
    protected ProcedureId parentGroupId;


    public ProcedureShadow(ProcedureId procId, IProcedureWithState liveProcedure, DefaultProcedureRegistry registry)
    {
        this.procedureId = Asserts.checkNotNull(procId);
        setProcedureRegistry(registry);
    }


    /*
     * Used when deserializing from data store
     */
    public void setProcedureRegistry(DefaultProcedureRegistry registry)
    {
        this.registry = Asserts.checkNotNull(registry, IProcedureRegistry.class);
    }


    public void connectLiveProcedure(IProcedureWithState proc)
    {
        Asserts.checkNotNull(proc, IProcedureWithState.class);

        this.ref = new WeakReference<>(proc);
        long lastUpdated = lastDescriptionUpdate;
        captureState();

        // listen to all procedure events
        eventSrcInfo = proc.getEventSourceInfo();
        eventPublisher = registry.getParentHub().getEventBus().getPublisher(eventSrcInfo);
        proc.registerListener(this);
        DefaultProcedureRegistry.log.debug("Procedure {} connected to shadow", procedureId.getUniqueID());

        // send procedure changed event if description has changed
        if (lastUpdated < lastDescriptionUpdate)
            eventPublisher.publish(new ProcedureChangedEvent(System.currentTimeMillis(), procedureId));
    }


    public void disconnectLiveProcedure(IProcedureWithState proc)
    {
        proc.unregisterListener(this);
        captureState();
        ref.clear();
        DefaultProcedureRegistry.log.debug("Procedure {} disconnected from shadow", procedureId.getUniqueID());
    }


    @Override
    public void handleEvent(Event e)
    {
        if (e instanceof ProcedureEvent)
        {
            // forward all procedure events to bus
            eventPublisher.publish(e);
            registry.eventPublisher.publish(e);
        }
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
        latestDescription = proc.getCurrentDescription();
        parentGroupId = proc.getParentGroupID();

        return previousUpdate < lastDescriptionUpdate;
    }


    @Override
    public ProcedureId getProcedureID()
    {
        return procedureId;
    }


    @Override
    public String getUniqueIdentifier()
    {
        return procedureId.getUniqueID();
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
    public Map<QName, Object> getProperties()
    {
        return getCurrentDescription().getProperties();
    }


    @Override
    public TimeExtent getValidTime()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null && proc.isEnabled())
            return proc.getValidTime();
        else if (latestDescription != null)
            return latestDescription.getValidTime();
        else
            return TimeExtent.beginAt(Instant.ofEpochMilli(lastDescriptionUpdate));
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
        if (proc != null && proc.isEnabled())
            return proc.getCurrentDescription();
        else
            return latestDescription;
    }


    @Override
    public IProcedureGroup<? extends IProcedureWithState> getParentGroup()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null && proc.isEnabled())
            return proc.getParentGroup();
        else if (parentGroupId != null)
            return (IProcedureGroup<?>)registry.getProcedureShadow(parentGroupId.getUniqueID());
        else
            return null;
    }


    @Override
    public ProcedureId getParentGroupID()
    {
        return parentGroupId;
    }


    @Override
    public long getLastDescriptionUpdate()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null && proc.isEnabled())
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
