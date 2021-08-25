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

import java.util.ArrayList;
import java.util.List;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.database.IProcedureObsDatabaseModule;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.ui.api.IModuleAdminPanel;
import org.sensorhub.ui.data.FieldProperty;
import org.sensorhub.ui.data.MyBeanItem;
import com.vaadin.event.Action;
import com.vaadin.event.Action.Handler;
import com.vaadin.server.FontAwesome;
import com.vaadin.server.ThemeResource;
import com.vaadin.shared.MouseEventDetails.MouseButton;
import com.vaadin.ui.Alignment;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.v7.ui.TreeTable;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window.CloseEvent;
import com.vaadin.ui.Window.CloseListener;


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
    private static final Action DELETE_PROCEDURE_ACTION = new Action("Delete All Procedure Data", new ThemeResource("icons/module_delete.png"));
    
    VerticalLayout layout;
    ProcedureSearchList procedureTable;
    TabSheet dataStreamTabs;
    
    
    @Override
    public void build(final MyBeanItem<ModuleConfig> beanItem, final IProcedureObsDatabaseModule<?> db)
    {
        super.build(beanItem, db);
        
        // assign default database number if not set and module hasn't been initialized yet
        if (!db.isInitialized() && db.getConfiguration().databaseNum == null)
        {
            int highestDbNum = 0;
            for (var otherDb: getParentHub().getDatabaseRegistry().getRegisteredObsDatabases())
                highestDbNum = Math.max(otherDb.getDatabaseNum(), highestDbNum);
            for (var otherDb: getParentHub().getModuleRegistry().getLoadedModules(IProcedureObsDatabase.class))
            {
                if (otherDb.getDatabaseNum() == null)
                    continue;
                highestDbNum = Math.max(otherDb.getDatabaseNum(), highestDbNum);
            }
            
            var nextDbNum = highestDbNum+1;
            //db.getConfiguration().databaseNum = nextDbNum;
            ((FieldProperty)beanItem.getItemProperty("databaseNum")).setValue(nextDbNum);
        }
        
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
                    if (event.getButton() == MouseButton.LEFT)
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
                }
            });
            
            // also add context menu
            procedureTable.getTable().addActionHandler(new Handler() {
                @Override
                public Action[] getActions(Object target, Object sender)
                {
                    List<Action> actions = new ArrayList<>(10);
                    actions.add(DELETE_PROCEDURE_ACTION);
                    return actions.toArray(new Action[0]);
                }

                @Override
                public void handleAction(Action action, Object sender, Object target)
                {
                    String uid = (String)((TreeTable)sender).getValue();
                    
                    final ConfirmDialog popup = new ConfirmDialog("Are you sure you want to remove all data associated with procedure:<br/><b>" + uid + "</b>");
                    popup.addCloseListener(new CloseListener() {
                        @Override
                        public void windowClose(CloseEvent e)
                        {
                            if (popup.isConfirmed())
                            {
                                try
                                {
                                    // log action
                                    //logAction(action, selectedModule);
                                    
                                    db.getDataStreamStore().removeEntries(new DataStreamFilter.Builder()
                                        .withProcedures(new ProcedureFilter.Builder()
                                            .withUniqueIDs(uid)
                                            .build())
                                        .build());
                                    
                                    db.getProcedureStore().remove(uid);
                                    
                                    procedureTable.updateTable(db, new ProcedureFilter.Builder().build());
                                }
                                catch (Exception e1)
                                {
                                    e1.printStackTrace();
                                }
                            }
                        }
                    });
                    
                    procedureTable.getUI().addWindow(popup);                    
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
                .withLimit(10)
                .build())
            .forEach(dsEntry -> {
                var dsID = dsEntry.getKey().getInternalID();
                var dsInfo = dsEntry.getValue();
                var dsPanel = new DatabaseStreamPanel(db, dsInfo, dsID);
                dataStreamTabs.addTab(dsPanel, dsPanel.getCaption(), FontAwesome.DATABASE);
            });
    }
}
