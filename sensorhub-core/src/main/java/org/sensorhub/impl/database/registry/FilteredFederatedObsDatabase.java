/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.registry;

import java.math.BigInteger;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.view.ProcedureObsDatabaseView;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;


/**
 * <p>
 * Extension of federated obs database allowing to filter out certain
 * databases or add new ones.
 * </p>
 *
 * @author Alex Robin
 * @since Dec 11, 2020
 */
public class FilteredFederatedObsDatabase extends FederatedObsDatabase
{
    final Set<Integer> unfilteredDatabases = Sets.newHashSet();
    final ObsFilter obsFilter;
    
    
    public FilteredFederatedObsDatabase(IDatabaseRegistry registry, ObsFilter obsFilter, int... unfilteredDatabases)
    {
        super(registry);
        
        this.obsFilter = obsFilter;
        for (int dbNum: unfilteredDatabases)
            this.unfilteredDatabases.add(dbNum);
    }
    

    @Override
    protected LocalDatabaseInfo getLocalDbInfo(long publicID)
    {
        var dbInfo = super.getLocalDbInfo(publicID);
        if (!unfilteredDatabases.contains(dbInfo.databaseNum))
            dbInfo.db = new ProcedureObsDatabaseView(dbInfo.db, obsFilter);
        return dbInfo;
    }
    

    @Override
    protected LocalDatabaseInfo getLocalDbInfo(BigInteger publicID)
    {
        var dbInfo = super.getLocalDbInfo(publicID);
        if (!unfilteredDatabases.contains(dbInfo.databaseNum))
            dbInfo.db = new ProcedureObsDatabaseView(dbInfo.db, obsFilter);
        return dbInfo;
    }
    

    @Override
    protected Collection<IProcedureObsDatabase> getAllDatabases()
    {
        var allDbs = super.getAllDatabases();
        
        return new AbstractCollection<>() {

            @Override
            public Iterator<IProcedureObsDatabase> iterator()
            {
                return Iterators.transform(allDbs.iterator(), db -> {
                    var dbNum = db.getDatabaseNum();
                    if (!unfilteredDatabases.contains(dbNum))
                        return new ProcedureObsDatabaseView(db, obsFilter);
                    return db;
                });
            }

            @Override
            public int size()
            {
                return allDbs.size();
            }            
        };
    }
    
    
    

}
