/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.resource;

import java.io.IOException;
import java.util.Collection;
import org.sensorhub.api.common.BigId;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.vast.util.Asserts;


/**
 * <p>
 * Base class for all resource formatter / parser
 * </p>
 * 
 * @param <K> Resource Key
 * @param <V> Resource Object
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public abstract class ResourceBinding<K, V>
{
    public static final String INDENT = "  ";
    
    
    protected final RequestContext ctx;
    protected final IdEncoder idEncoder; // encoder/decoder to obfuscate ids provided by service
    
    
    protected ResourceBinding(RequestContext ctx, IdEncoder idEncoder)
    {
        this.ctx = Asserts.checkNotNull(ctx, RequestContext.class);
        this.idEncoder = Asserts.checkNotNull(idEncoder, IdEncoder.class);
    }
    
    
    public abstract V deserialize() throws IOException;
    public abstract void serialize(K key, V res, boolean showLinks) throws IOException;
    public abstract void startCollection() throws IOException;
    public abstract void endCollection(Collection<ResourceLink> links) throws IOException;
    
    
    public BigId decodeID(String encodedID)
    {
        return idEncoder.decodeID(encodedID);
    }
    
    
    public String encodeID(BigId decodedID)
    {
        return idEncoder.encodeID(decodedID);
    }
}