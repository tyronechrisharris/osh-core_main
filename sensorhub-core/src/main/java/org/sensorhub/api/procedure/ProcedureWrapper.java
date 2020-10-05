/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Wrapper class for AbstractProcess implementing {@link IProcedureWithDesc}
 * </p>
 *
 * @author Alex Robin
 * @date Oct 4, 2020
 */
public class ProcedureWrapper implements IProcedureWithDesc
{
    AbstractProcess fullDesc;


    public ProcedureWrapper(AbstractProcess fullDesc)
    {
        this.fullDesc = Asserts.checkNotNull(fullDesc, AbstractProcess.class);
    }
    
    
    @Override
    public String getUniqueIdentifier()
    {
        return fullDesc.getUniqueIdentifier();
    }


    @Override
    public String getName()
    {
        return fullDesc.getName();
    }


    @Override
    public String getDescription()
    {
        return fullDesc.getDescription();
    }
    

    @Override
    public TimeExtent getValidTime()
    {
        return fullDesc.getValidTime();
    }


    @Override
    public AbstractProcess getFullDescription()
    {
        return fullDesc;
    }


    @Override
    public boolean hasFullDescription()
    {
        return fullDesc != null;
    }
}
