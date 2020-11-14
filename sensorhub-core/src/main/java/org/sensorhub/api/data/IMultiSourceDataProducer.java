/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.data;

import java.util.Collection;
import org.sensorhub.api.procedure.IProcedureGroupDriver;
import org.sensorhub.api.procedure.ProcedureId;


/**
 * <p>
 * Interface for multi-source data producers.
 * </p><p>
 * This type of producer can be used to model an entire group of data sources
 * (e.g. sensor network) and provides additional methods to filter data records
 * and metadata by the source procedure ID.
 * </p>
 *
 * @author Alex Robin
 * @since May 31, 2015
 */
public interface IMultiSourceDataProducer extends IDataProducer, IProcedureGroupDriver<IDataProducer>
{

    /**
     * Get procedures that are observing the specified feature of interest.
     * @param foiUID Unique ID of feature of interest
     * @return Read-only collection of procedure unique IDs (can be empty)
     */
    public Collection<ProcedureId> getProceduresWithFoi(String foiUID);

}
