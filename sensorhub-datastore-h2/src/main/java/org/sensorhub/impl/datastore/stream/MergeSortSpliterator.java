/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.stream;

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.function.Consumer;


/**
 * <p>
 * Implementation of spliterator used to merge and sort elements obtained
 * from several pre-ordered spliterators. The source spliterators must
 * already be ordered according to the specified Comparator.
 * </p>
 * @param <T> 
 * 
 * @author Alex Robin
 * @date Apr 6, 2018
 */
public class MergeSortSpliterator<T> implements Spliterator<T>
{
    Collection<Spliterator<T>> iterators;
    PriorityQueue<StreamSource> sources;
    Comparator<T> comparator;
    boolean needInit;
    
    
    private class StreamSource implements Comparable<StreamSource>
    {
        Spliterator<T> iterator;
        T nextRecord;
        
        StreamSource(Spliterator<T> iterator)
        {
            this.iterator = iterator;
        }
        
        @Override
        public int compareTo(StreamSource other)
        {
            return comparator.compare(nextRecord, other.nextRecord);
        }       
        
        boolean tryAdvance()
        {
            return iterator.tryAdvance(elt -> nextRecord = elt);
        }
    }


    public MergeSortSpliterator(Collection<Spliterator<T>> iterators, Comparator<T> comparator)
    {
        this.iterators = iterators;
        this.comparator = comparator;
        this.needInit = true;
    }


    @Override
    public int characteristics()
    {
        return Spliterator.SORTED;
    }


    @Override
    public Comparator<? super T> getComparator()
    {
        return comparator;
    }


    @Override
    public long estimateSize()
    {
        return 0;
    }


    @Override
    public boolean tryAdvance(Consumer<? super T> action)
    {
        if (needInit)
            loadFirstElements();
        
        // get next element from queue
        StreamSource nextSource = sources.poll();
        if (nextSource == null)
            return false;
        
        // apply action
        action.accept(nextSource.nextRecord);
        
        // if an element was found, prepare for next iteration by fetching
        // the next element on the corresponding iterator.
        if (nextSource.tryAdvance())
            sources.add(nextSource);
                
        return true;
    }
    
    
    private void loadFirstElements()
    {
        this.sources = new PriorityQueue<>(iterators.size());
        
        for (Spliterator<T> it: iterators)
        {
            StreamSource src = new StreamSource(it);
            if (src.tryAdvance())
                sources.add(src);
        }
        
        iterators.clear();
        iterators = null;
        needInit = false;
    }
    

    @Override
    public Spliterator<T> trySplit()
    {
        return null;
    }
}