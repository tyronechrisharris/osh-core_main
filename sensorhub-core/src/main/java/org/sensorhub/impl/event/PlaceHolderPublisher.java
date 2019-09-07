/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.event;

import java.util.ArrayList;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiPredicate;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventPublisher;


/**
 * <p>
 * Class that temporarily holds list of subscribers before the publisher 
 * actually registers
 * </p>
 *
 * @author Alex Robin
 * @date Feb 21, 2019
 */
public class PlaceHolderPublisher implements IEventPublisher
{
    ArrayList<Subscriber<? super Event>> subscribers = new ArrayList<>();
    
    
    @Override
    public void subscribe(Subscriber<? super Event> subscriber)
    {
        subscribers.add(subscriber);      
    }
    
    
    public void transferTo(IEventPublisher realPublisher)
    {
        for (Subscriber<? super Event> s: subscribers)
            realPublisher.subscribe(s);
    }


    @Override
    public void publish(Event e)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void publish(Event e, BiPredicate<Subscriber<? super Event>, ? super Event> onDrop)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public int getNumberOfSubscribers()
    {
        return subscribers.size();
    }

}
