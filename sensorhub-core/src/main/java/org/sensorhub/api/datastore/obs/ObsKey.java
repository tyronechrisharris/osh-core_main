/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore.obs;

import java.time.Instant;
import java.util.Objects;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;


/**
 * <p>
 * Immutable key object used to index observations in storage.<br/>
 * When the key is used for retrieval, the internalID is used if provided,
 * otherwise all other parameters are used.
 * </p>
 *
 * @author Alex Robin
 * @since Mar 19, 2018
 */
public class ObsKey
{
    protected long dataStreamID = 0;
    protected long foiID = IObsData.NO_FOI;
    protected Instant resultTime = null;
    protected Instant phenomenonTime = null;


    protected ObsKey()
    {
    }


    public ObsKey(long dataStreamID, Instant phenomenonTime)
    {
        Asserts.checkArgument(dataStreamID > 0, "data stream ID must be > 0");
        this.dataStreamID = dataStreamID;
        this.phenomenonTime = Asserts.checkNotNull(phenomenonTime, "phenomenonTime");
    }


    public ObsKey(long dataStreamID, long foiID, Instant phenomenonTime)
    {
        this(dataStreamID, phenomenonTime);
        this.foiID = foiID;
    }


    public ObsKey(long dataStreamID, long foiID, Instant resultTime, Instant phenomenonTime)
    {
        this(dataStreamID, foiID, phenomenonTime);
        this.resultTime = resultTime;
    }


    /**
     * @return The ID of the data stream that the observation is part of.
     */
    public long getDataStreamID()
    {
        return dataStreamID;
    }


    /**
     * @return The internal ID of the feature of interest that was observed.
     */
    public long getFoiID()
    {
        return foiID;
    }


    /**
     * @return The time of occurrence of the measured phenomenon (e.g. for
     * many automated sensor devices, this is typically the sampling time).<br/>
     * This field cannot be null.
     */
    public Instant getPhenomenonTime()
    {
        return phenomenonTime;
    }


    /**
     * @return The time at which the observation result was obtained.<br/>
     * This is typically the same as the phenomenon time for many automated
     * in-situ and remote sensors doing the sampling and actual measurement
     * (almost) simultaneously, but different for measurements made in a lab on
     * samples that were collected previously. It is also different for models
     * and simulations outputs (e.g. for a model, this is the run time).<br/>
     * If no result time was explicitly set, this returns the phenomenon time
     */
    public Instant getResultTime()
    {
        if (resultTime == null || resultTime == Instant.MIN)
            return phenomenonTime;
        return resultTime;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }


    @Override
    public int hashCode()
    {
        return java.util.Objects.hash(
            getDataStreamID(),
            getFoiID(),
            getPhenomenonTime(),
            getResultTime());
    }


    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof ObsKey))
            return false;

        ObsKey other = (ObsKey)obj;
        return Objects.equals(getDataStreamID(), other.getDataStreamID()) &&
               Objects.equals(getFoiID(), other.getFoiID()) &&
               Objects.equals(getPhenomenonTime(), other.getPhenomenonTime()) &&
               Objects.equals(getResultTime(), other.getResultTime());
    }
}
