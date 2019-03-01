/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.impl.datastore.stream.MergeSortSpliterator;
import static org.junit.Assert.*;


public class TestMergeSortSpliterator
{

    @Before
    public void init()
    {
        System.gc();
    }
    
    
    private void checkSortOrder(Spliterator<Integer> mergedIt, int numElements)
    {
        final AtomicInteger counter = new AtomicInteger(0);
        StreamSupport.stream(mergedIt, false)
                     //.peek(System.out::println)
                     .peek(e -> counter.incrementAndGet())
                     .reduce((last, next) -> {
                         if (Integer.compare(last,  next) > 0)
                             throw new IllegalStateException("Stream is not sorted");
                         return next;
                     });
        
        assertEquals(numElements, counter.get());
        
        System.gc();
        System.out.println("Total memory is " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }
    
    
    @Test
    public void testSortFewItems()
    {
        ArrayList<Spliterator<Integer>> sources = new ArrayList<>();
        sources.add(Arrays.asList(1,4,9).spliterator());
        sources.add(Arrays.asList(2,5,7).spliterator());
        sources.add(Arrays.asList(3,6,8).spliterator());
        
        MergeSortSpliterator<Integer> mergedIt = new MergeSortSpliterator<>(sources, Integer::compare);
        checkSortOrder(mergedIt, 9);
    }
    
    
    @Test
    public void testSortWithDuplicates()
    {
        ArrayList<Spliterator<Integer>> sources = new ArrayList<>();
        sources.add(Arrays.asList(1,4,9).spliterator());
        sources.add(Arrays.asList(2,4,5,7).spliterator());
        sources.add(Arrays.asList(3,6,8,9).spliterator());
        
        MergeSortSpliterator<Integer> mergedIt = new MergeSortSpliterator<>(sources, Integer::compare);
        checkSortOrder(mergedIt, 11);
    }
    
    
    @Test
    public void testSortManyItemsFromFewSources()
    {
        ArrayList<Spliterator<Integer>> sources = new ArrayList<>();
        sources.add(Stream.iterate(0, e -> e+3).limit(1000).spliterator());
        sources.add(Stream.iterate(1, e -> e+3).limit(1000).spliterator());
        sources.add(Stream.iterate(2, e -> e+3).limit(1000).spliterator());
        
        MergeSortSpliterator<Integer> mergedIt = new MergeSortSpliterator<>(sources, Integer::compare);
        checkSortOrder(mergedIt, 3000);
    }
    
    
    @Test
    public void testSortFromVeryDifferentSizeSources()
    {
        ArrayList<Spliterator<Integer>> sources = new ArrayList<>();
        sources.add(Stream.iterate(0, e -> e+3).limit(400).spliterator());
        sources.add(Stream.iterate(1, e -> e+3).limit(1000).spliterator());
        sources.add(Stream.iterate(2, e -> e+3).limit(100).spliterator());
        
        MergeSortSpliterator<Integer> mergedIt = new MergeSortSpliterator<>(sources, Integer::compare);
        checkSortOrder(mergedIt, 1500);
    }
    
    
    @Test
    public void testSortManyItemsFromManySources()
    {
        ArrayList<Spliterator<Integer>> sources = new ArrayList<>();
        int numSources = 1000;
        for (int i=0; i<numSources;i++)
            sources.add(Stream.iterate(i, e -> e+numSources).limit(1000).spliterator());
        
        MergeSortSpliterator<Integer> mergedIt = new MergeSortSpliterator<>(sources, Integer::compare);
        checkSortOrder(mergedIt, 1000000);
    }
    
    
    @Test
    public void testSortManyItemsFromManySourcesRandom()
    {
        ArrayList<Spliterator<Integer>> sources = new ArrayList<>();
        int numSources = 10000;
        for (int i=0; i<numSources;i++)
        {
            int step = (int)Math.random()*numSources;
            sources.add(Stream.iterate(i, e -> e+step).limit(200).spliterator());
        }
        
        MergeSortSpliterator<Integer> mergedIt = new MergeSortSpliterator<>(sources, Integer::compare);
        checkSortOrder(mergedIt, 2000000);
    }
    
    
    @Test(expected = IllegalStateException.class)
    public void testOutOfOrderSources()
    {
        ArrayList<Spliterator<Integer>> sources = new ArrayList<>();
        sources.add(Arrays.asList(1,9,4).spliterator());
        sources.add(Arrays.asList(2,5,7).spliterator());
        sources.add(Arrays.asList(3,6,8).spliterator());
        
        MergeSortSpliterator<Integer> mergedIt = new MergeSortSpliterator<>(sources, Integer::compare);
        checkSortOrder(mergedIt, 9);
    }

}
