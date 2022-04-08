/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.vast.util.Asserts;


public class ObsSystemDbWrapper
{
    IObsSystemDatabase readDb;
    IObsSystemDatabase writeDb;
    IdConverter idConverter;
    IDatabaseRegistry dbRegistry;
    
    
    public ObsSystemDbWrapper(IObsSystemDatabase readDb, IObsSystemDatabase writeDb, IDatabaseRegistry dbRegistry)
    {
        Asserts.checkNotNull(readDb);
        this.readDb = readDb;
        this.writeDb = writeDb;
        this.dbRegistry = Asserts.checkNotNull(dbRegistry, IDatabaseRegistry.class);
        
        // init public <-> internal ID converter
        this.idConverter = new DatabaseRegistryIdConverter(
            Asserts.checkNotNull(dbRegistry, IDatabaseRegistry.class),
            writeDb != null ? writeDb.getDatabaseNum() : 0);
    }


    public IObsSystemDatabase getReadDb()
    {
        return readDb;
    }


    public IObsSystemDatabase getWriteDb()
    {
        return writeDb;
    }


    public IdConverter getIdConverter()
    {
        return idConverter;
    }
    
    
    public IDatabaseRegistry getDatabaseRegistry()
    {
        return dbRegistry;
    }

}
