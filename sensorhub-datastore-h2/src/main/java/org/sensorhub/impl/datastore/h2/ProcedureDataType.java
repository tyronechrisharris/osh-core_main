/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.Instant;


/**
 * <p>
 * H2 DataType implementation for FoiPeriod objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
public class ProcedureDataType extends KryoDataType
{
    ProcedureDataType()
    {
        // pre-register known types with Kryo
        registeredClasses.put(20, Instant.class);
        
        // register serializer for Guava Range objects
        //serializers.put(Range.class, new KryoUtils.TimeRangeSerializer());
    }
}