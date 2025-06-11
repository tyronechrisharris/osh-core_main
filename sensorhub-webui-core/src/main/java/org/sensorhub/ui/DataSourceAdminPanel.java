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

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Timer;
import java.util.TimerTask;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import org.sensorhub.api.data.IDataProducerModule;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.api.sensor.ISensorModule;
import org.sensorhub.ui.api.IModuleAdminPanel;
import org.sensorhub.ui.data.MyBeanItem;
import com.vaadin.server.FontAwesome;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Layout;
import com.vaadin.ui.Panel;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.v7.ui.ListSelect;
import com.vaadin.server.VaadinSession;


/**
 * <p>
 * Admin panel for data source modules.<br/>
 * This adds a section to view structure of outputs, and view output data values.
 * </p>
 * 
 * @param <ModuleType> Type of module handled by this panel 
 *
 * @author Alex Robin
 * @since 1.0
 */
@SuppressWarnings("serial")
public class DataSourceAdminPanel<ModuleType extends IDataProducerModule<?>> extends DefaultModulePanel<ModuleType> implements IModuleAdminPanel<ModuleType>
{
    private transient ResourceBundle resourceBundle;
    public static final int REFRESH_TIMEOUT = 3*AdminUIModule.HEARTBEAT_INTERVAL*1000;
    Panel obsPanel, statusPanel;
    Map<String, DataComponent> outputBuffers = new HashMap<>();
    
    
    static class Spacing extends Label
    {
        public Spacing()
        {
            setHeight(0, Unit.PIXELS);
        }
    }
    
    
    @Override
    public void build(final MyBeanItem<ModuleConfig> beanItem, final ModuleType module)
    {
        super.build(beanItem, module);
        this.resourceBundle = ResourceBundle.getBundle("org.sensorhub.ui.messages", VaadinSession.getCurrent().getLocale());
        
        // sensor info panel
        if (module.isInitialized())
        {
            Label sectionLabel = new Label(resourceBundle.getString("dataSourceAdminPanel.dataSourceInfo"));
            sectionLabel.addStyleName(STYLE_H3);
            sectionLabel.addStyleName(STYLE_COLORED);
            addComponent(sectionLabel);
            addComponent(new Label("<b>" + resourceBundle.getString("dataSourceAdminPanel.uniqueId") + "</b> " + module.getUniqueIdentifier(), ContentMode.HTML));
            
            // display list of FOIs
            var fois = module.getCurrentFeaturesOfInterest().keySet();
            if (fois != null && !fois.isEmpty())
            {
                addComponent(new Label("<b>" + resourceBundle.getString("dataSourceAdminPanel.foiIds") + "</b>", ContentMode.HTML));
                ListSelect list = new ListSelect();
                list.setRows(4);
                list.setNullSelectionAllowed(false);
                for (var foi: fois)
                    list.addItem(foi);
                addComponent(list);
            }
            
            // outputs section
            if (!module.getOutputs().isEmpty())
            {
                // title
                addComponent(new Spacing());
                HorizontalLayout titleBar = new HorizontalLayout();
                titleBar.setSpacing(true);
                sectionLabel = new Label(resourceBundle.getString("dataSourceAdminPanel.outputs"));
                sectionLabel.addStyleName(STYLE_H3);
                sectionLabel.addStyleName(STYLE_COLORED);
                titleBar.addComponent(sectionLabel);
                titleBar.setComponentAlignment(sectionLabel, Alignment.MIDDLE_LEFT);
                
                // refresh button
                final Timer timer = new Timer();
                final Button refreshButton = new Button(resourceBundle.getString("dataSourceAdminPanel.refreshButton"));
                refreshButton.setDescription(resourceBundle.getString("dataSourceAdminPanel.refreshButtonDescription"));
                refreshButton.setIcon(REFRESH_ICON);
                refreshButton.addStyleName(STYLE_SMALL);
                refreshButton.addStyleName(STYLE_QUIET);
                refreshButton.setData(false);
                titleBar.addComponent(refreshButton);
                titleBar.setComponentAlignment(refreshButton, Alignment.MIDDLE_LEFT);
                addComponent(titleBar);
                
                refreshButton.addClickListener(new ClickListener() {
                    transient TimerTask autoRefreshTask;                    
                    @Override
                    public void buttonClick(ClickEvent event)
                    {
                        // toggle button state
                        boolean state = !(boolean)refreshButton.getData();
                        refreshButton.setData(state);
                        
                        if (state)
                        {
                            autoRefreshTask = new TimerTask()
                            {
                                @Override
                                public void run()
                                {
                                    final UI ui = DataSourceAdminPanel.this.getUI();
                                    long now = System.currentTimeMillis();
                                    
                                    if (ui != null && (now - ui.getLastHeartbeatTimestamp()) < REFRESH_TIMEOUT)
                                    {                                        
                                        ui.access(new Runnable() {
                                            @Override
                                            public void run()
                                            {
                                                rebuildOutputsPanels(module);
                                                UI.getCurrent().push();
                                            }
                                        });
                                    }
                                    else
                                        cancel(); // if panel was detached
                                }
                            };
                            timer.schedule(autoRefreshTask, 0L, 1000L);
                            refreshButton.setIcon(FontAwesome.TIMES);
                            refreshButton.setCaption(resourceBundle.getString("dataSourceAdminPanel.stopButton"));
                        }
                        else
                        {
                            autoRefreshTask.cancel();
                            refreshButton.setIcon(REFRESH_ICON);
                            refreshButton.setCaption(resourceBundle.getString("dataSourceAdminPanel.refreshButton"));
                        }
                    }
                });               
                        
                // output panels
                rebuildOutputsPanels(module);
            }
        }
    }
    
        
    protected void rebuildOutputsPanels(IDataProducerModule<?> module)
    {
        if (module != null)
        {
            if (module instanceof ISensorModule)
            {
                obsPanel = newOutputsPanel(resourceBundle.getString("dataSourceAdminPanel.observationOutputs"), ((ISensorModule<?>) module).getObservationOutputs(), obsPanel);
                statusPanel = newOutputsPanel(resourceBundle.getString("dataSourceAdminPanel.statusOutputs"), ((ISensorModule<?>) module).getStatusOutputs(), statusPanel);
            }
            else
            {
                obsPanel = newOutputsPanel(resourceBundle.getString("dataSourceAdminPanel.outputs"), module.getOutputs(), obsPanel);
            }
        }
    }
    
    
    protected Panel newOutputsPanel(String name, Map<String, ? extends IStreamingDataInterface> outputs, Panel oldPanel)
    {
        Panel newPanel = null;
        
        if (!outputs.isEmpty())
        {
            newPanel = newPanel(name);
            for (IStreamingDataInterface output: outputs.values())
            {
                // used cached output component if available
                DataComponent dataStruct = outputBuffers.get(output.getName());                    
                if (dataStruct == null)
                {
                    dataStruct = output.getRecordDescription().copy();
                    outputBuffers.put(dataStruct.getName(), dataStruct);
                }
                    
                // load latest data into component
                DataBlock latestRecord = output.getLatestRecord();
                if (latestRecord != null)
                    dataStruct.setData(latestRecord);
                
                // data structure
                Component sweForm = new SWECommonForm(dataStruct);
                ((Layout)newPanel.getContent()).addComponent(sweForm);
            }
            
            if (oldPanel != null)
                replaceComponent(oldPanel, newPanel);
            else
                addComponent(newPanel);
        }
        
        return newPanel;
    }
    
    
    protected Panel newPanel(String title)
    {
        Panel panel = new Panel(title);
        VerticalLayout layout = new VerticalLayout();
        layout.setSizeFull();
        layout.setMargin(true);
        layout.setSpacing(true);
        layout.setDefaultComponentAlignment(Alignment.TOP_LEFT);
        panel.setContent(layout);
        return panel;
    }
}
