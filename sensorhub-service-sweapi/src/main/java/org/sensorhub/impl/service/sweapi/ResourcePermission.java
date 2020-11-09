/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import org.sensorhub.api.security.IPermission;
import org.sensorhub.impl.security.AbstractPermission;
import org.vast.util.Asserts;


/**
 * <p>
 * Top level permission for a resource.<br/>
 * </p>
 *
 * @author Alex Robin
 * @since Nov 6, 2018
 */
public class ResourcePermission extends AbstractPermission
{
    long resourceID;
    
    
    public ResourcePermission(long resourceID)
    {
        Asserts.checkArgument(resourceID > 0);
        this.resourceID = resourceID;
    }


    @Override
    public boolean implies(IPermission perm)
    {
        return perm instanceof ResourcePermission && resourceID == ((ResourcePermission)perm).resourceID;
    }
}
