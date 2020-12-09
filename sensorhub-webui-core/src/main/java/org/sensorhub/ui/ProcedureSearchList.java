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

import java.util.ArrayList;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.ui.api.UIConstants;
import com.vaadin.ui.Component;
import com.vaadin.v7.data.Item;
import com.vaadin.v7.data.Property.ValueChangeEvent;
import com.vaadin.v7.data.Property.ValueChangeListener;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.v7.ui.Table;
import com.vaadin.v7.ui.TextField;
import com.vaadin.ui.VerticalLayout;


public class ProcedureSearchList extends VerticalLayout
{
    final static String PROC_UID_PROP = "uid";
    final static String PROC_NAME_PROP = "name";
    final static String PROC_DESC_PROP = "desc";
    
    Table table;
    
    
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
            .withLimit(10)
            .build());
    }
    
    
    protected Component buildProcedureTable(final IProcedureObsDatabase db, final ItemClickListener selectionListener)
    {
        table = new Table();
        table.setWidth(100, Unit.PERCENTAGE);
        table.setPageLength(5);
        table.setSelectable(true);
        table.addStyleName(UIConstants.STYLE_SMALL);
        
        // add column names
        table.addContainerProperty(PROC_UID_PROP, String.class, null, "Procedure UID", null, null);
        table.addContainerProperty(PROC_NAME_PROP, String.class, null, "Name", null, null);
        table.addContainerProperty(PROC_DESC_PROP, String.class, null, "Description", null, null);
        
        table.addItemClickListener(selectionListener);
        
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
}
