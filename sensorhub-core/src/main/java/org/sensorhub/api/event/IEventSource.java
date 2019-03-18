/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.event;


/**
 * <p>
 * Interface for all event sources capable of describing themselves
 * </p>
 *
 * @author Alex Robin
 * @date Mar 5, 2019
 */
public interface IEventSource
{

    /**
     * Get the descriptor of this event source, including the source ID and 
     * the group ID it should be attached to. This information is used when
     * publishing or subscribing to the event bus.
     * @return the event source descriptor
     */
    IEventSourceInfo getEventSourceInfo();

}