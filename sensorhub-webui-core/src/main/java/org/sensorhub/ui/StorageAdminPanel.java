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

import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.api.persistence.IRecordStoreInfo;
import org.sensorhub.api.persistence.IRecordStorageModule;
import org.sensorhub.ui.api.IModuleAdminPanel;
import org.sensorhub.ui.data.MyBeanItem;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;
import com.vaadin.ui.VerticalLayout;


/**
 * <p>
 * Admin panel for storage modules.<br/>
 * This adds a section to view storage content in a table + histograms to
 * view the distribution of data records over time
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since 1.0
 */
@SuppressWarnings("serial")
public class StorageAdminPanel extends DefaultModulePanel<IRecordStorageModule<?>> implements IModuleAdminPanel<IRecordStorageModule<?>>
{
       
    
    @Override
    public void build(final MyBeanItem<ModuleConfig> beanItem, final IRecordStorageModule<?> storage)
    {
        super.build(beanItem, storage);       
        
        if (storage != null)
        {            
            // section layout
            final VerticalLayout form = new VerticalLayout();
            form.setWidth(100.0f, Unit.PERCENTAGE);
            form.setMargin(false);
            form.setSpacing(true);
            
            // section title
            form.addComponent(new Label(""));
            HorizontalLayout titleBar = new HorizontalLayout();
            titleBar.setSpacing(true);
            Label sectionLabel = new Label("Data Store Content");
            sectionLabel.addStyleName(STYLE_H3);
            sectionLabel.addStyleName(STYLE_COLORED);
            titleBar.addComponent(sectionLabel);
            titleBar.setComponentAlignment(sectionLabel, Alignment.MIDDLE_LEFT);
            
            // refresh button to show latest record
            Button refreshButton = new Button("Refresh");
            refreshButton.setDescription("Reload data from storage");
            refreshButton.setIcon(REFRESH_ICON);
            refreshButton.addStyleName(STYLE_SMALL);
            refreshButton.addStyleName(STYLE_QUIET);
            titleBar.addComponent(refreshButton);
            titleBar.setComponentAlignment(refreshButton, Alignment.MIDDLE_LEFT);
            refreshButton.addClickListener(new ClickListener() {
                private static final long serialVersionUID = 1L;
                @Override
                public void buttonClick(ClickEvent event)
                {
                    buildDataPanel(form, storage);
                }
            });
                    
            form.addComponent(titleBar);
            
            // add I/O panel
            buildDataPanel(form, storage);
            addComponent(form);
        }
    }
    
    
    protected void buildDataPanel(VerticalLayout form, final IRecordStorageModule<?> storage)
    {        
        // measurement outputs
        int i = 1;        
        if (storage.isStarted())
        {
            for (IRecordStoreInfo dsInfo: storage.getRecordStores().values())
            {
                Panel panel = new StorageStreamPanel(i++, storage, dsInfo);
                
                // we start at index 2 because there is a spacer and the title on top
                if (form.getComponentCount() > i)
                {
                    Component oldPanel = form.getComponent(i);
                    form.replaceComponent(oldPanel, panel);
                }
                else
                    form.addComponent(panel);
            }
        }
    }
}
