/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.registry;

import org.sensorhub.impl.datastore.AbstractTestDataStreamStore;
import org.sensorhub.impl.datastore.mem.InMemoryDataStreamStore;
import org.sensorhub.impl.datastore.mem.InMemoryObsStore;


public class TestInMemDataStreamStore extends AbstractTestDataStreamStore<InMemoryDataStreamStore>
{
       
    
    protected InMemoryDataStreamStore initStore() throws Exception
    {
        return (InMemoryDataStreamStore)new InMemoryObsStore().getDataStreams();
    }
    
    
    protected void forceReadBackFromStorage()
    {
    }
    
    
    protected void testAddAndSelectLatestValidTime_ExpectedResults()
    {
        addToExpectedResults(2, 4, 7);
    }
    
    
    protected void testAddAndSelectByTimeRange_ExpectedResults(int testCaseIdx)
    {
        switch (testCaseIdx)
        {
            case 1: addToExpectedResults(1, 3, 5, 7); break;
            case 2: addToExpectedResults(1, 7); break;
            case 3: addToExpectedResults(7); break;
        }        
    }
    
    
    @Override
    protected void testAddAndSelectByOutputName_ExpectedResults()
    {
        addToExpectedResults(1, 3, 5);
    }
    
    
    protected void testAddAndSelectByProcedureID_ExpectedResults()
    {
        addToExpectedResults(3, 4, 5);
    }
    
    
    protected void testAddAndSelectByKeywords_ExpectedResults(int testCaseIdx)
    {
        switch (testCaseIdx)
        {
            case 1: addToExpectedResults(4, 5); break;
            case 2: addToExpectedResults(1, 4, 5); break;
            case 3: addToExpectedResults(2, 4, 5); break;
            case 4: addToExpectedResults(4); break;
        }        
    }
}
