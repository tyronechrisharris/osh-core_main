/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.h2.mvstore;

import java.util.function.BiPredicate;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;


/**
 * <p>
 * Modified version of H2 MVMap to optimize certain operations
 * </p>
 *
 * @author Alex Robin
 * @param <K> Key type
 * @param <V> Value type
 * @date Apr 19, 2018
 */
public class MVBTreeMap<K, V> extends MVMap<K, V>
{

    protected MVBTreeMap(DataType keyType, DataType valueType)
    {
        super(keyType, valueType);
    }


    public Entry<K, V> getEntry(K key)
    {
        return getEntry(root, key);
    }


    /*
     * Same code as binarySearch method but returning full entry
     */
    @SuppressWarnings({ "unchecked" })
    private Entry<K, V> getEntry(Page p, K key)
    {
        int x = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            p = p.getChildPage(x);
            return getEntry(p, key);
        }
        if (x >= 0) {
            return new DataUtils.MapEntry<>((K)p.getKey(x), (V)p.getValue(x));
        }
        return null;
    }


    public Entry<K, V> ceilingEntry(K key)
    {
        return getMinMaxEntry(root, key, false, false);
    }


    public Entry<K, V> floorEntry(K key)
    {
        return getMinMaxEntry(root, key, true, false);
    }
    
    
    /*
     * Same code as getMinMax method but returning full entry
     */
    @SuppressWarnings("unchecked")
    private Entry<K, V> getMinMaxEntry(Page p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
            int x = p.binarySearch(key);
            if (x < 0) {
                x = -x - (min ? 2 : 1);
            } else if (excluding) {
                x += min ? -1 : 1;
            }
            if (x < 0 || x >= p.getKeyCount()) {
                return null;
            }
            return new DataUtils.MapEntry<>((K)p.getKey(x), (V)p.getValue(x));
        }
        int x = p.binarySearch(key);
        if (x < 0) {
            x = -x - 1;
        } else {
            x++;
        }
        while (true) {
            if (x < 0 || x >= getChildPageCount(p)) {
                return null;
            }
            Entry<K, V> entry = getMinMaxEntry(p.getChildPage(x), key, min, excluding);
            if (entry != null) {
                return entry;
            }
            x += min ? -1 : 1;
        }
    }
    
    
    protected boolean containsKey(Page p, Object key) {
        int x = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            p = p.getChildPage(x);
            return containsKey(p, key);
        }
        if (x >= 0) {
            return true;
        }
        return false;
    }
    

    @Override
    public boolean containsKey(Object key) {
        return containsKey(root, key);
    }
    
    
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        if (!containsKey(key))
            return null;
        
        beforeWrite();
        V result = null;
        long v = writeVersion;
        synchronized (this) {
            Page p = root.copy(v);
            result = (V) remove(p, v, key);
            if (!p.isLeaf() && p.getTotalCount() == 0) {
                p.removePage();
                p = Page.createEmpty(this,  p.getVersion());
            }
            newRoot(p);
        }
        return result;
    }
    

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        if (containsKey(key))
            return get(key);
        else
            return put(key, value);
    }
    
    
    /**
     * Associates the new value to the given key only if the specified
     * predicate is true
     * @param key
     * @param value
     * @param predicate Bi-Predicate taking the old and new value
     * @return the old value if the key existed, or null otherwise
     */
    public V putWithCondition(K key, V value, BiPredicate<V, V> predicate)
    {
        V oldValue = get(key);
        if (predicate.test(oldValue, value))
            put(key, value);
        return oldValue;
    }
    
    
    /**
     * Add or update a key-value pair.
     *
     * @param p the page
     * @param writeVersion the write version
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null
     */
    @Override
    protected Object put(Page p, long writeVersion, Object key, Object value) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
                return null;
            }
            p.setKey(index, key); // updating key is needed in case parts are not used for comparison
            return p.setValue(index, value);
        }
        // p is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = p.getChildPage(index).copy(writeVersion);
        if (c.getMemory() > store.getPageSplitSize() && c.getKeyCount() > 1) {
            // split on the way down
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            Page split = c.split(at);
            p.setChild(index, split);
            p.insertNode(index, k, c);
            // now we are not sure where to add
            return put(p, writeVersion, key, value);
        }
        Object result = put(c, writeVersion, key, value);
        p.setChild(index, c);
        return result;
    }
    

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class Builder<K, V> implements MapBuilder<MVBTreeMap<K, V>, K, V>
    {
        protected DataType keyType;
        protected DataType valueType;


        public Builder<K, V> keyType(DataType keyType)
        {
            this.keyType = keyType;
            return this;
        }


        public Builder<K, V> valueType(DataType valueType)
        {
            this.valueType = valueType;
            return this;
        }


        @Override
        public MVBTreeMap<K, V> create()
        {
            if (keyType == null)
                keyType = new ObjectDataType();

            if (valueType == null)
                valueType = new ObjectDataType();

            return new MVBTreeMap<>(keyType, valueType);
        }
    }
}
