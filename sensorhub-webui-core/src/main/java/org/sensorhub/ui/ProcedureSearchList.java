/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.ui;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Objects;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.ui.api.UIConstants;
import org.vast.util.TimeExtent;
import com.vaadin.ui.Component;
import com.vaadin.v7.data.Item;
import com.vaadin.v7.data.Property.ValueChangeEvent;
import com.vaadin.v7.data.Property.ValueChangeListener;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.v7.ui.Table;
import com.vaadin.v7.ui.TextField;
import com.vaadin.v7.ui.TreeTable;
import com.vaadin.ui.VerticalLayout;


public class ProcedureSearchList extends VerticalLayout
{
    final static String PROP_PROC_UID = "uid";
    final static String PROP_PROC_NAME = "name";
    final static String PROP_PROC_VALID = "valid";
    final static String PROP_PROC_DESC = "desc";
    
    TreeTable table;
    
    
    public ProcedureSearchList(final IProcedureObsDatabase db, final ItemClickListener selectionListener)
    {
        setMargin(false);
        
        // procedure uid / search box
        final TextField searchBox = new TextField("Search Procedures");
        searchBox.addStyleName(UIConstants.STYLE_SMALL);
        searchBox.setDescription("UID prefix or keywords to search for procedures");
        searchBox.setValue("*");
        addComponent(searchBox);
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
        addComponent(buildProcedureTable(db, selectionListener));
        
        // populate table with 10 first results
        updateTable(db, new ProcedureFilter.Builder()
            .build());
    }
    
    
    protected Component buildProcedureTable(final IProcedureObsDatabase db, final ItemClickListener selectionListener)
    {
        table = new TreeTable();
        table.setWidth(100, Unit.PERCENTAGE);
        table.setPageLength(5);
        table.setSelectable(true);
        table.addStyleName(UIConstants.STYLE_SMALL);
        
        // add column names
        table.addContainerProperty(PROP_PROC_UID, String.class, null, "Procedure UID", null, null);
        table.addContainerProperty(PROP_PROC_NAME, String.class, null, "Name", null, null);
        table.addContainerProperty(PROP_PROC_VALID, String.class, null, "Validity", null, null);
        table.addContainerProperty(PROP_PROC_DESC, String.class, null, "Description", null, null);
        
        table.addItemClickListener(selectionListener);
        
        return table;
    }
    
    
    protected void updateTable(final IProcedureObsDatabase db, final ProcedureFilter procFilter)
    {
        table.removeAllItems();
        db.getProcedureStore().select(procFilter)
            .forEach(proc -> {
                String itemId = proc.getUniqueIdentifier();
                Item item = table.addItem(itemId);
                
                if (item != null)
                {
                    item.getItemProperty(PROP_PROC_UID).setValue(proc.getUniqueIdentifier());
                    item.getItemProperty(PROP_PROC_NAME).setValue(proc.getName());
                    item.getItemProperty(PROP_PROC_VALID).setValue(getValidTimeString(proc));
                    item.getItemProperty(PROP_PROC_DESC).setValue(proc.getDescription());
                    table.setChildrenAllowed(itemId, false);
                }
                else
                {
                    item = table.getItem(itemId);
                    table.setChildrenAllowed(itemId, true);
                    
                    // also show all historical version as children
                    String childId = proc.getUniqueIdentifier() + "_" + proc.getValidTime();
                    Item childItem = table.addItem(childId);
                    childItem.getItemProperty(PROP_PROC_NAME).setValue(proc.getName());
                    childItem.getItemProperty(PROP_PROC_VALID).setValue(getValidTimeString(proc));
                    childItem.getItemProperty(PROP_PROC_DESC).setValue(proc.getDescription());
                    table.setParent(childId, itemId);
                    table.setChildrenAllowed(childId, false);
                }
            });
    }
    
    
    protected String getValidTimeString(IProcedureWithDesc proc)
    {
        var validTime = proc.getValidTime();
        if (validTime == null)
            return "ALWAYS";
        
        return validTime.begin().truncatedTo(ChronoUnit.SECONDS) + " / " +
               (validTime.endsNow() ? "now" : validTime.end().truncatedTo(ChronoUnit.SECONDS));
    }
    
    
    public TreeTable getTable()
    {
        return table;
    }
}
