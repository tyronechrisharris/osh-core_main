/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.config.DisplayInfo.Required;
import org.sensorhub.api.datastore.DatabaseConfig;


/**
 * <p>
 * Config class for {@link MVObsDatabase} module
 * </p>
 *
 * @author Alex Robin
 * @date Sep 23, 2019
 */
public class MVObsDatabaseConfig extends DatabaseConfig
{
    
    @Required
    @DisplayInfo(desc="Path to database file")
    public String storagePath;
    
    
    @DisplayInfo(desc="Memory cache size for page chunks, in KB")
    public int memoryCacheSize = 5*1024;
    
    
    @DisplayInfo(desc="Size of the auto-commit write buffer, in KB")
    public int autoCommitBufferSize = 1024;
    
        
    @DisplayInfo(desc="Set to compress underlying file storage")
    public boolean useCompression = false;
    
    
    @DisplayInfo(desc="Set to enable spatial indexing of individual observations sampling locations (when provided)")
    public boolean indexObsLocation = false;
}
