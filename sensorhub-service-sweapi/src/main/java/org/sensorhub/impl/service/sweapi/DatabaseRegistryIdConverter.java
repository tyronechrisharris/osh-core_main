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

import java.math.BigInteger;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of resource ID converter that just delegates to the 
 * database registry
 * </p>
 *
 * @author Alex Robin
 * @since Dec 23, 2020
 */
public class DatabaseRegistryIdConverter implements IdConverter
{
    final IDatabaseRegistry registry;
    final int databaseNum;
    
    
    DatabaseRegistryIdConverter(IDatabaseRegistry registry, int databaseNum)
    {
        this.registry = Asserts.checkNotNull(registry, IDatabaseRegistry.class);
        this.databaseNum = databaseNum;
    }


    @Override
    public long toInternalID(long publicID)
    {
        return registry.getLocalID(databaseNum, publicID);
    }


    @Override
    public long toPublicID(long internalID)
    {
        return registry.getPublicID(databaseNum, internalID);
    }


    @Override
    public BigInteger toInternalID(BigInteger publicID)
    {
        return registry.getLocalID(databaseNum, publicID);
    }


    @Override
    public BigInteger toPublicID(BigInteger internalID)
    {
        return registry.getPublicID(databaseNum, internalID);
    }
}
