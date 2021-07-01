/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.config.DisplayInfo.Required;
import org.sensorhub.api.database.DatabaseConfig;


/**
 * <p>
 * Base config class for MVStore based database modules
 * </p>
 *
 * @author Alex Robin
 * @date Nov 20, 2020
 */
public abstract class MVDatabaseConfig extends DatabaseConfig
{
    public enum IdProviderType
    {
        SEQUENTIAL,
        UID_HASH
    }
    
    
    @Required
    @DisplayInfo(desc = "Path to database file")
    public String storagePath;
    
    
    @DisplayInfo(desc = "Memory cache size for page chunks, in KB")
    public int memoryCacheSize = 5 * 1024;
    
    
    @DisplayInfo(desc = "Size of the auto-commit write buffer, in KB")
    public int autoCommitBufferSize = 1024;
    
    
    @DisplayInfo(label = "ID Generator", desc = "Method used to generate new resource IDs")
    public IdProviderType idProviderType = IdProviderType.SEQUENTIAL;
    
    
    @DisplayInfo(desc = "Set to compress underlying file storage")
    public boolean useCompression = false;
    
    
    @DisplayInfo(desc = "Set to open the database as read-only")
    public boolean readOnly = false;

}