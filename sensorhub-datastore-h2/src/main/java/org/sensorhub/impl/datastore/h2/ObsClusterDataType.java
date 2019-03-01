/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.Instant;
import org.vast.util.Bbox;
import com.google.common.collect.Range;


/**
 * <p>
 * H2 DataType implementation for ObsCluster objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class ObsClusterDataType extends KryoDataType
{
    ObsClusterDataType()
    {
        // pre-register known types with Kryo
        registeredClasses.put(20, Range.class);
        registeredClasses.put(21, Instant.class);
        registeredClasses.put(22, Bbox.class);
    }
}