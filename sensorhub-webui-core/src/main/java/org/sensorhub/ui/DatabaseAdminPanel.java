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
import org.sensorhub.api.database.IDatabase;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.database.IProcedureObsDatabaseModule;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.ui.api.IModuleAdminPanel;
import org.sensorhub.ui.api.UIConstants;
import org.sensorhub.ui.data.MyBeanItem;
import org.vast.swe.ScalarIndexer;
import com.vaadin.server.FontAwesome;
import com.vaadin.server.Sizeable.Unit;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.v7.data.Item;
import com.vaadin.v7.data.Property.ValueChangeEvent;
import com.vaadin.v7.data.Property.ValueChangeListener;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.v7.ui.Table;
import com.vaadin.v7.ui.TextField;
import net.opengis.swe.v20.DataComponent;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;
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
    private final static String PROC_UID_PROP = "uid";
    private final static String PROC_NAME_PROP = "name";
    private final static String PROC_DESC_PROP = "desc";
    
    VerticalLayout layout;
    Table table;
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
            
            /*// refresh button to show latest record
            Button refreshButton = new Button("Refresh");
            refreshButton.setDescription("Reload data from database");
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
                    buildDataPanel(form, db);
                }
            });*/
                    
            layout.addComponent(titleBar);
            buildProcedureSelectionPanel(db);
            
            dataStreamTabs = new TabSheet();
            layout.addComponent(dataStreamTabs);
            
            addComponent(layout);
        }
    }
    
    
    protected void buildProcedureSelectionPanel(final IProcedureObsDatabase db)
    {        
        // procedure uid / search box
        final TextField searchBox = new TextField("Search Procedures");
        searchBox.addStyleName(UIConstants.STYLE_SMALL);
        searchBox.setDescription("UID prefix or keywords to search for procedures");
        searchBox.setValue("*");
        layout.addComponent(searchBox);
        searchBox.addValueChangeListener(new ValueChangeListener() {
            @Override
            public void valueChange(ValueChangeEvent event)
            {
                String txt = searchBox.getValue().trim();
                
                // build lists of uids and keywords
                var tokens = txt.split(" |,");
                var uids = new ArrayList<String>();
                var keywords = new ArrayList<String>();
                for (var t: tokens)
                {
                    if (txt.startsWith("urn:") || txt.equals("*"))
                        uids.add(t);
                    else if (t.length() > 0)
                        keywords.add(t);
                }
                
                var procFilter = new ProcedureFilter.Builder();
                if (!uids.isEmpty())
                    procFilter.withUniqueIDs(uids);
                if (!keywords.isEmpty())
                    procFilter.withKeywords(keywords);
                
                // update table
                updateTable(db, procFilter.build());
            }
        });
        
        // procedure table
        layout.addComponent(buildProcedureTable(db));
        
        // populate table with 10 first results
        updateTable(db, new ProcedureFilter.Builder()
            .withLimit(10)
            .build());
    }
    
    
    protected Component buildProcedureTable(final IProcedureObsDatabase db)
    {
        table = new Table();
        table.setWidth(100, Unit.PERCENTAGE);
        table.setPageLength(5);
        table.setSelectable(true);
        
        // add column names
        table.addContainerProperty(PROC_UID_PROP, String.class, null, "Procedure UID", null, null);
        table.addContainerProperty(PROC_NAME_PROP, String.class, null, "Name", null, null);
        table.addContainerProperty(PROC_DESC_PROP, String.class, null, "Description", null, null);
        
        table.addItemClickListener(new ItemClickListener()
        {
            @Override
            public void itemClick(ItemClickEvent event)
            {
                try
                {
                    // select and open module configuration
                    String procUID = (String)event.getItem().getItemProperty(PROC_UID_PROP).getValue();
                    showProcedureData(db, procUID);
                }
                catch (Exception e)
                {
                    DisplayUtils.showErrorPopup("Unexpected error when selecting module", e);
                }
            }
        });
        
        return table;
    }
    
    
    protected void updateTable(final IProcedureObsDatabase db, final ProcedureFilter procFilter)
    {
        table.removeAllItems();
        db.getProcedureStore().select(procFilter)
            .forEach(proc -> {
                Item item = table.addItem(proc.getUniqueIdentifier());
                item.getItemProperty(PROC_UID_PROP).setValue(proc.getUniqueIdentifier());
                item.getItemProperty(PROC_NAME_PROP).setValue(proc.getName());
                item.getItemProperty(PROC_DESC_PROP).setValue(proc.getDescription());                        
            });
    }
    
    
    protected void showProcedureData(final IProcedureObsDatabase db, String procUID)
    {
        // show in tabs
        dataStreamTabs.removeAllComponents();
                
        /*// remove previous panels
        var it = layout.getComponentIterator();
        for (int i=0; i<layout.getComponentCount(); i++)
        {
            var comp = layout.getComponent(i);
            if (comp instanceof StorageStreamPanel)
                layout.removeComponent(comp);
        */
        
        db.getDataStreamStore().selectEntries(new DataStreamFilter.Builder()
                .withProcedures().withUniqueIDs(procUID).done()
                .build())
            .forEach(dsEntry -> {
                var dsID = dsEntry.getKey().getInternalID();
                var dsInfo = dsEntry.getValue();
                var dsPanel = new DatabaseDataStreamPanel(db, dsInfo, dsID);
                //layout.addComponent(dsPanel);                
                dataStreamTabs.addTab(dsPanel, dsPanel.getCaption(), FontAwesome.DATABASE);
            });
    }
}
