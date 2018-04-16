/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;


/**
 * <p>
 * Generic utility method to deal with objects
 * </p>
 *
 * @author Alex Robin
 * @date Mar 19, 2018
 */
public class ObjectUtils
{
    
    private ObjectUtils() {};
    
    
    public static String toString(Object object, boolean recursive)
    {
        if (object == null)
            return "null";

        Class<?> clazz = object.getClass();
        StringBuilder sb = new StringBuilder(clazz.getSimpleName()).append(" {");

        while (clazz != null && !clazz.equals(Object.class))
        {
            Field[] fields = clazz.getDeclaredFields();
            for (Field f : fields)
            {
                if (!Modifier.isStatic(f.getModifiers()))
                {
                    try
                    {
                        f.setAccessible(true);
                        sb.append(f.getName()).append(": ").append(f.get(object)).append(",");
                    }
                    catch (IllegalAccessException e)
                    {                        
                    }
                }
            }

            if (!recursive)
            {
                break;
            }
            clazz = clazz.getSuperclass();
        }

        sb.deleteCharAt(sb.lastIndexOf(","));
        return sb.append("}").toString();
    }

}
