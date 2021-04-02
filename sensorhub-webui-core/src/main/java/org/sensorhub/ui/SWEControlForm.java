/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.ui;

import org.sensorhub.api.command.CommandData;
import org.sensorhub.api.command.IStreamingControlInterface;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import net.opengis.swe.v20.DataComponent;


@SuppressWarnings("serial")
public class SWEControlForm extends SWEEditForm
{
    transient IStreamingControlInterface controlInput;
    transient DataComponent controlSink;
    
    
    public SWEControlForm(final IStreamingControlInterface controlInput)
    {
        super(controlInput.getCommandDescription().copy());
        this.controlInput = controlInput;
        this.component.assignNewDataBlock();
        buildForm();
    }
    
    
    public SWEControlForm(final DataComponent params)
    {
        super(params.copy());
        this.addSpacing = true;
        this.controlSink = params;
        this.component.setData(params.getData());
        buildForm();
    }
    
    
    protected void buildForm()
    {
        super.buildForm();
        
        // send button
        Button sendBtn = new Button("Send Command");
        addComponent(sendBtn);
        setComponentAlignment(sendBtn, Alignment.MIDDLE_LEFT);
        sendBtn.addClickListener(new ClickListener()
        {   
            private static final long serialVersionUID = 1L;

            @Override
            public void buttonClick(ClickEvent event)
            {
                try
                {
                    if (controlInput != null)
                    {
                        var cmd = new CommandData(0, component.getData());
                        controlInput.executeCommand(cmd, ack -> {}).get();
                    }
                    else
                        controlSink.setData(component.getData());
                }
                catch (Exception e)
                {
                    DisplayUtils.showErrorPopup("Error while sending command to sensor", e);
                }
            }
        });
    }
}
