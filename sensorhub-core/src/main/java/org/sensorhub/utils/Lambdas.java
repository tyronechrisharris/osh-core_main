/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.utils;

import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * Util class with lambda support functions such as helper methods
 * for catching exceptions in Lambdas.
 * </p>
 *
 * @author Alex Robin
 * @since Feb 2, 2022
 */
public class Lambdas
{

    public interface ThrowingRunnable
    {
        public void run() throws Exception;
    }
    
    
    public interface ThrowingConsumer<T>
    {
        public void accept(T t) throws Exception;
    }
    
    
    public interface ThrowingFunction<T, R>
    {
        public R apply(T t) throws Exception;
    }
    
    
    private Lambdas() {}
    
    
    /**
     * Return a {@link Runnable} that catches all checked exceptions
     * and wraps them inside of an unchecked {@link CallbackException}
     * @param r The checked runnable that can throw an exception
     * @return A regular {@link Runnable}
     */
    public static final Runnable checked(ThrowingRunnable r)
    {
        return () -> {
            try
            {
                r.run();
            }
            catch (Exception e)
            {
                throw new CallbackException(e);
            }
        };
    }
    
    
    /**
     * Return a {@link Consumer} that catches all checked exceptions
     * and wraps them inside of an unchecked {@link CallbackException}
     * @param c The checked consumer that can throw an exception
     * @return A regular {@link Consumer}
     */
    public static final <T> Consumer<T> checked(ThrowingConsumer<T> c)
    {
        return t -> {
            try
            {
                c.accept(t);
            }
            catch (Exception e)
            {
                throw new CallbackException(e);
            }
        };
    }
    
    
    /**
     * Return a {@link Function} that catches all checked exceptions
     * and wraps them inside of an unchecked {@link CallbackException}
     * @param f The checked function that can throw an exception
     * @return A regular {@link Function}
     */
    public static final <T, R> Function<T, R> checked(ThrowingFunction<T, R> f)
    {
        return t -> {
            try
            {
                return f.apply(t);
            }
            catch (Exception e)
            {
                throw new CallbackException(e);
            }
        };
    }
}
