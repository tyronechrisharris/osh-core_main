/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.utils;

import java.util.concurrent.ConcurrentSkipListMap;


/**
 * <p>
 * Implementation of a concurrent tree map that can also accept and match
 * toward entries with containing wildcards.
 * <p></p>
 * When an entry with a wildcard is inserted, the get method will return
 * its value for any key that has the wildcard key as prefix.
 * </p>
 * 
 * @param <V> Value Type
 * 
 * @author Alex Robin
 * @date Dec 5, 2020
 */
@SuppressWarnings("serial")
public class MapWithWildcards<V> extends ConcurrentSkipListMap<String, V>
{
    static final String WILDCARD_CHAR = "*";
    static final String END_PREFIX_CHAR = "\0";
    
    
    @Override
    public V get(Object obj)
    {
        var key = (String)obj;
        
        var e = floorEntry(key);
        if (e == null)
            return null;
        
        var floorKey = e.getKey();
        
        // case of wildcard match
        if (floorKey.endsWith(END_PREFIX_CHAR))
        {
            String prefix = floorKey.substring(0, floorKey.length()-1);
            if (key.startsWith(prefix))
                return e.getValue(); 
        }
        
        // case of exact match
        else if (floorKey.equals(key))
        {
            return e.getValue();
        }
        
        return null;
    }
    
    
    @Override
    public V put(String key, V value)
    {
        // replace wildcard with special char so it gets sorted before anything else with that prefix
        if (key.endsWith(WILDCARD_CHAR))
            key = key.substring(0, key.length()-1) + END_PREFIX_CHAR;
        
        return super.put(key, value);
    }
    
    
    @Override
    public V putIfAbsent(String key, V value)
    {
        var oldValue = get(key);
        if (oldValue == null)
            return put(key, value);
        else
            return oldValue;
    }
}
