/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.

 Contributor(s): 
    Alexandre Robin "alex.robin@sensiasoft.com"
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;


public class IdFilter implements Predicate<String>
{
    private Set<String> idList = new HashSet<>();
    
    
    public Set<String> getIdList()
    {
        return idList;
    }


    @Override
    public boolean test(String id)
    {
        return idList.contains(id);
    }
}
