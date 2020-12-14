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

import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.database.IProcedureObsDatabaseModule;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.ui.api.IModuleAdminPanel;
import org.sensorhub.ui.data.MyBeanItem;
import com.vaadin.server.FontAwesome;
import com.vaadin.ui.Alignment;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


/**
 * <p>
 * Admin panel for database modules.<br/>
 * This adds a section to view storage content in a table + histograms to
 * view the distribution of data records over time
 * </p>
 *
 * @author Alex Robin
 * @since 1.0
 */
@SuppressWarnings("serial")
public class DatabaseAdminPanel extends DefaultModulePanel<IProcedureObsDatabaseModule<?>> implements IModuleAdminPanel<IProcedureObsDatabaseModule<?>>
{
    VerticalLayout layout;
    ProcedureSearchList procedureTable;
    TabSheet dataStreamTabs;
    
    
    @Override
    public void build(final MyBeanItem<ModuleConfig> beanItem, final IProcedureObsDatabaseModule<?> db)
    {
        super.build(beanItem, db);
        
        if (db != null && db.isStarted())
        {
            // section layout
            layout = new VerticalLayout();
            layout.setWidth(100.0f, Unit.PERCENTAGE);
            layout.setMargin(false);
            layout.setSpacing(true);
            
            // section title
            //layout.addComponent(new Label(""));
            HorizontalLayout titleBar = new HorizontalLayout();
            titleBar.setSpacing(true);
            Label sectionLabel = new Label("Database Content");
            sectionLabel.addStyleName(STYLE_H3);
            sectionLabel.addStyleName(STYLE_COLORED);
            titleBar.addComponent(sectionLabel);
            titleBar.setComponentAlignment(sectionLabel, Alignment.MIDDLE_LEFT);
            layout.addComponent(titleBar);
            
            procedureTable = new ProcedureSearchList(db, new ItemClickListener() {
                @Override
                public void itemClick(ItemClickEvent event)
                {
                    try
                    {
                        // select and open module configuration
                        String procUID = (String)event.getItem().getItemProperty(ProcedureSearchList.PROP_PROC_UID).getValue();
                        if (procUID != null)
                            showProcedureData(db, procUID);
                    }
                    catch (Exception e)
                    {
                        DisplayUtils.showErrorPopup("Unexpected error when selecting procedure", e);
                    }
                }
            });
            layout.addComponent(procedureTable);
            
            dataStreamTabs = new TabSheet();
            layout.addComponent(dataStreamTabs);
            
            addComponent(layout);
        }
    }
    
    
    protected synchronized void showProcedureData(final IProcedureObsDatabase db, String procUID)
    {
        // remove previous tabs
        dataStreamTabs.removeAllComponents();
        
        // show in tabs
        db.getDataStreamStore().selectEntries(new DataStreamFilter.Builder()
                .withProcedures().withUniqueIDs(procUID).done()
                .build())
            .forEach(dsEntry -> {
                var dsID = dsEntry.getKey().getInternalID();
                var dsInfo = dsEntry.getValue();
                var dsPanel = new DatabaseStreamPanel(db, dsInfo, dsID);
                dataStreamTabs.addTab(dsPanel, dsPanel.getCaption(), FontAwesome.DATABASE);
            });
    }
}
