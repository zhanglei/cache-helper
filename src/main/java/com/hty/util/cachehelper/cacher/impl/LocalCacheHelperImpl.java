package com.hty.util.cachehelper.cacher.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import com.hty.util.cachehelper.SerializeUtil;
import com.hty.util.cachehelper.bean.SortedSetBean;
import com.hty.util.cachehelper.cacher.LocalCacheHelper;
import com.hty.util.cachehelper.util.Md5Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 仿照Jedis缓存的本地内存中实现
 * @author Hetianyi 2017/12/30
 * @version 1.0
 */
@SuppressWarnings("unchecked")
public class LocalCacheHelperImpl implements LocalCacheHelper {

    private static final Logger logger = LoggerFactory.getLogger(JedisHelperImpl.class);
    /**
     * 指定自身对象
     */
    private static LocalCacheHelper localCacheHelper;
    /**
     * 存放对象的K-V容器
     */
    private Map<String, Object> ObjectKVData =
            Collections.synchronizedMap(new HashMap<String, Object>());
    ;
    /**
     * 存放Hash结构的K-V容器
     */
    private Map<String, Map<Object, Object>> hashData =
            Collections.synchronizedMap(new HashMap<String, Map<Object, Object>>());
    /**
     * 记录需要过期的键和过期时间
     */
    private Map<String, Long> expiredKeyMap =
            Collections.synchronizedMap(new HashMap<String, Long>());

    private LocalCacheHelperImpl() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new CleanKeyTask(), 0, 10000);
    }

    /**
     * 获取单例的LocalCacheHelper
     */
    public static synchronized LocalCacheHelper getInstance() {
        if (null == localCacheHelper) {
            localCacheHelper = new LocalCacheHelperImpl();
        }
        return localCacheHelper;
    }


    /**
     * 校验key或者Hash的field，如果key或field为空，则抛出异常
     */
    private void assertKey(byte[] keyorfield) {
        if (null == keyorfield) {
            throw new IllegalArgumentException("Key or field cannot be null!");
        }
    }

    /**
     * 校验key或者Hash的field，如果key或field为空，则抛出异常
     */
    private void assertKey(String keyorfield) {
        if (null == keyorfield) {
            throw new IllegalArgumentException("Key or field cannot be null!");
        }
    }

    /**
     * 比较两个字节数组是否相等
     */
    private boolean byteEqual(byte[] a, byte[] b) {
        if (null == a && null == b) return true;
        if (null == a && null != b) return false;
        if (null == b && null != a) return false;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    @Override
    public void set(String key, String value) {
        assertKey(key);
        if (null == value) {
            this.del(key);
            return;
        }
        this.setEX(key, value, 0);
    }

    @Override
    public void setEX(String key, String value, int sec) {
        assertKey(key);
        if (null == value) {
            this.del(key);
            return;
        }
        ObjectKVData.put(key, value);
        if (sec > 0) {
            expiredKeyMap.put(key, System.currentTimeMillis() + 1000 * sec);
        } else if (expiredKeyMap.containsKey(key)) {
            expiredKeyMap.remove(key);
        }
    }

    @Override
    public String get(String key) {
        if (isKeyExpired(key))
            return null;
        return (String) ObjectKVData.get(key);
    }

    @Override
    public List<String> mget(String... keys) {
        if (null == keys || keys.length == 0) {
            return null;
        }
        List<String> list = new ArrayList<String>();
        for (String s : keys) {
            if (isKeyExpired(s))
                list.add(null);
            list.add((String) ObjectKVData.get(s));
        }
        return list;
    }

    @Override
    public String hget(String key, String field) {
        if (isKeyExpired(key))
            return null;
        Map<Object, Object> itemMap = hashData.get(key);
        if (null != itemMap)
            return itemMap.get(field) == null ? null : itemMap.get(field).toString();
        return null;
    }

    @Override
    public Map<String, String> hmget(String key, String... field) {
        if (isKeyExpired(key))
            return null;
        Map<Object, Object> itemMap = hashData.get(key);
        if (null != itemMap && null != field) {
            Map<String, String> ret = new HashMap<String, String>(field.length);
            for (String f : field) {
                ret.put(f, itemMap.get(field) == null ? null : itemMap.get(field).toString());
            }
            return ret;
        }
        return null;
    }

    @Override
    public void hset(String key, String field, String value) {
        assertKey(key);
        assertKey(field);
        Map<Object, Object> itemMap = hashData.get(key);
        if (null == itemMap) {
            itemMap = new HashMap<Object, Object>();
            hashData.put(key, itemMap);
        }
        if (null == value) {
            this.hdel(key, field);
            return;
        }
        itemMap.put(field, value);
    }

    /**
     * 保存Hash对象时对象的键field为field的md5值
     */
    @Override
    public void hset(String key, Object field, Object value) {
        assertKey(key);
        Map<Object, Object> itemMap = hashData.get(key);
        if (null == itemMap) {
            itemMap = new HashMap<Object, Object>();
            hashData.put(key, itemMap);
        }
        if (null == value) {
            this.hdel(key, field);
            return;
        }
        byte[] fieldBytes = SerializeUtil.serialize(field);
        assertKey(fieldBytes);
        String fieldMd5 = Md5Util.getMd5(fieldBytes);
        itemMap.put(fieldMd5, value);
    }


    @Override
    public void setMap(String key, Map<?, ?> map) {
        assertKey(key);
        if (null == map) {
            this.hashData.remove(key);
            return;
        }
        for (Entry<?, ?> item : map.entrySet()) {
            this.hset(key, item.getKey(), item.getValue());
        }
    }

    @Override
    public <T> T getMapValue(String mapKey, Object fieldKey, Class<T> type) {
        if (null == mapKey || null == fieldKey) return null;
        if (isKeyExpired(mapKey))
            return null;
        Map<Object, Object> itemMap = hashData.get(mapKey);
        if (null == itemMap)
            return null;
        byte[] fieldBytes = SerializeUtil.serialize(fieldKey);
        String fieldMd5 = Md5Util.getMd5(fieldBytes);
        return (T) itemMap.get(fieldMd5);
    }


    @Override
    public void appendMapItem(String mapKey, Object fieldKey, Object value) {
        Map<Object, Object> map = this.hashData.get(mapKey);
        if (null == map) {
            map = new HashMap<Object, Object>();
        }
        byte[] fieldBytes = SerializeUtil.serialize(fieldKey);
        assertKey(fieldBytes);
        String fieldMd5 = Md5Util.getMd5(fieldBytes);
        map.put(fieldMd5, value);
    }

    @Override
    public <T> List<T> getMapValues(String mapKey, Class<T> type) {
        if (isKeyExpired(mapKey))
            return null;
        Map<Object, Object> map = this.hashData.get(mapKey);
        List<T> list = null;
        if (null != map) {
            list = new ArrayList<T>();
            for (Object o : map.values()) {
                if (null == o)
                    list.add(null);
                else
                    list.add((T) o);
            }
        }
        return list;
    }

    @Override
    public <T, K> Map<K, T> getMultiMapValues(String mapKey, Class<T> type, K... keys) {
        if (isKeyExpired(mapKey))
            return null;
        Map<Object, Object> map = this.hashData.get(mapKey);
        if (null != map && null != keys) {
            Map<K, T> ret = new HashMap<K, T>(keys.length);
            for (K o : keys) {
                byte[] fieldBytes = SerializeUtil.serialize(o);
                String fieldMd5 = Md5Util.getMd5(fieldBytes);
                ret.put(o, (T) map.get(fieldMd5));
            }
            return ret;
        }
        return null;
    }

    @Override
    public void setObject(String key, Object obj) {
        this.ObjectKVData.put(key, obj);
    }

    @Override
    public void setObjectEX(String key, Object value, int sec) {
        this.setObject(key, value);
        if (sec > 0) {
            expiredKeyMap.put(key, System.currentTimeMillis() + 1000 * sec);
        } else if (expiredKeyMap.containsKey(key)) {
            expiredKeyMap.remove(key);
        }
    }

    @Override
    public <T> T getObject(String key, Class<T> type) {
        if (isKeyExpired(key))
            return null;
        return (T) this.ObjectKVData.get(key);
    }


    @Override
    public void setStringList(String key, List<String> list) {
        this.ObjectKVData.put(key, list);
    }

    @Override
    public void appendStringListItem(String key, boolean tail, String... strings) {
        if (null == strings || strings.length == 0)
            return;
        List<String> list = (List<String>) this.ObjectKVData.get(key);
        if (null == list) {
            list = new ArrayList<String>();
            this.ObjectKVData.put(key, list);
        }
        for (String s : strings) {
            if (tail)
                list.add(s);
            else
                list.add(0, s);
        }
    }

    @Override
    public String getStringListItem(String key, int index) {
        if (isKeyExpired(key))
            return null;
        List<String> list = (List<String>) this.ObjectKVData.get(key);
        if (null == list || list.isEmpty() || list.size() <= index)
            return null;
        return list.get(index);
    }

    @Override
    public List<String> getStringListItems(String key, int start, int end) {
        if (isKeyExpired(key))
            return null;
        List<String> list = (List<String>) this.ObjectKVData.get(key);
        if (null != list && !list.isEmpty()) {
            if (end == -1) {
                if (list.size() <= start)
                    return null;
                return list.subList(start, list.size());
            } else {
                if (start >= end)
                    return null;
                return list.subList(start, list.size() < end ? list.size() : end);
            }
        }
        return null;
    }


    /////////////////////////////////////////////////////////////Object List

    /**
     * {@inheritDoc}
     * 本地缓存器缓存Object将原List中的对象替换成DataContainer，其中包裹数据对象，并将其序列化。<br>
     * 在内存中的状态是List&lt;byte[]&gt;形式的。
     */
    @Override
    public void setObjectList(String key, List<?> list) {
        if (null != list) {
            List<byte[]> dataformBytes = new ArrayList<byte[]>();
            for (int i = 0; i < list.size(); i++) {
                byte[] bs = SerializeUtil.serialize(list.get(i));
                dataformBytes.add(bs);
            }
            this.ObjectKVData.put(key, dataformBytes);
        } else
            this.ObjectKVData.put(key, list);
    }

    @Override
    public <T> T getObjectListItem(String key, int index, Class<T> type) {
        if (isKeyExpired(key))
            return null;
        List<byte[]> list = (List<byte[]>) this.ObjectKVData.get(key);
        if (null == list || list.isEmpty() || list.size() <= index)
            return null;
        byte[] bs = list.get(index);
        return (T) SerializeUtil.deserialize(bs, type);
    }

    @Override
    public <T> List<T> getObjectListItems(String key, int start, int end,
                                          Class<T> type) {
        if (isKeyExpired(key))
            return null;
        List<byte[]> list = (List<byte[]>) this.ObjectKVData.get(key);
        if (null != list && !list.isEmpty()) {
            if (end == -1) {
                if (list.size() <= start)
                    return null;
                list = list.subList(start, list.size());
                List<T> retList = new ArrayList<T>();
                for (int i = 0; i < list.size(); i++)
                    retList.add(SerializeUtil.deserialize(list.get(i), type));
                return retList;
            } else {
                if (start >= end)
                    return null;
                list = list.subList(start, list.size() < end ? list.size() : end);
                List<T> retList = new ArrayList<T>();
                for (int i = 0; i < list.size(); i++)
                    retList.add(SerializeUtil.deserialize(list.get(i), type));
                return retList;
            }
        }
        return null;
    }

    @Override
    public void appendObjectListItem(String key, boolean tail, Object... items) {
        if (null == items || items.length == 0)
            return;
        List<byte[]> list = (List<byte[]>) this.ObjectKVData.get(key);
        if (null == list) {
            list = new ArrayList<byte[]>();
            this.ObjectKVData.put(key, list);
        }
        for (Object s : items) {
            byte[] bs = SerializeUtil.serialize(s);
            if (tail)
                list.add(bs);
            else
                list.add(0, bs);
        }
    }

    @Override
    public void trimList(String key, int start, int end) {
        if (isKeyExpired(key))
            return;
        List<Object> list = (List<Object>) this.ObjectKVData.get(key);
        if (null == list || list.isEmpty())
            return;
        synchronized (list) {
            if (end == -1) {
                if (list.size() <= start)
                    return;
                list = list.subList(start, list.size());
            } else {
                if (start >= end)
                    return;
                list = list.subList(start, end >= list.size() ? list.size() : end);
            }
            this.ObjectKVData.put(key, list);
        }
    }

    @Override
    public void removeRepeatStringListItem(String key, int count, String value) {
        if (isKeyExpired(key))
            return;
        List<String> list = (List<String>) this.ObjectKVData.get(key);
        if (null == list || list.isEmpty())
            return;
        int list_size = list.size();
        int hit_count = 0;
        synchronized (list) {
            if (count > 0) {
                for (int i = 0; i < list_size; i++) {
                    if ((null == value && value == list.get(i)) || (null != value && value.equals(list.get(i)))) {
                        list.remove(i);
                        list_size--;
                        hit_count++;
                        i--;
                        if (hit_count == count)
                            break;
                    }
                }
            } else if (count < 0) {
                for (int i = list_size - 1; i >= 0; i--) {
                    if ((null == value && value == list.get(i)) || (null != value && value.equals(list.get(i)))) {
                        list.remove(i);
                        list_size--;
                        hit_count++;
                        if (hit_count == -count)
                            break;
                    }
                }
            } else {
                for (int i = 0; i < list_size; i++) {
                    if ((null == value && value == list.get(i)) || (null != value && value.equals(list.get(i)))) {
                        list.remove(i);
                        list_size--;
                        i--;
                    }
                }
            }
        }
    }

    @Override
    public void removeRepeatObjectListItem(String key, int count, Object value) {
        if (isKeyExpired(key))
            return;
        List<byte[]> list = (List<byte[]>) this.ObjectKVData.get(key);
        if (null == list || list.isEmpty())
            return;
        int list_size = list.size();
        int hit_count = 0;
        synchronized (list) {
            byte[] valuebs = SerializeUtil.serialize(value);
            if (count > 0) {
                for (int i = 0; i < list_size; i++) {
                    if ((null == value && value == list.get(i)) || (null != value && Arrays.equals(valuebs, list.get(i)))) {
                        list.remove(i);
                        list_size--;
                        hit_count++;
                        i--;
                        if (hit_count == count)
                            break;
                    }
                }
            } else if (count < 0) {
                for (int i = list_size - 1; i >= 0; i--) {
                    if ((null == value && value == list.get(i)) || (null != value && Arrays.equals(valuebs, list.get(i)))) {
                        list.remove(i);
                        list_size--;
                        hit_count++;
                        if (hit_count == -count)
                            break;
                    }
                }
            } else {
                for (int i = 0; i < list_size; i++) {
                    if ((null == value && value == list.get(i)) || (null != value && Arrays.equals(valuebs, list.get(i)))) {
                        list.remove(i);
                        list_size--;
                        i--;
                    }
                }
            }
        }
    }

    @Override
    public long getListLength(String key) {
        if (isKeyExpired(key))
            return 0;
        List<Object> list = (List<Object>) this.ObjectKVData.get(key);
        if (null == list)
            return 0;
        return list.size();
    }


    ///////////////////////////////////////////////////////////////////////////////////  Set


    @Override
    public <T> void setObjectSet(String key, Set<T> sets) {
        if (null == sets)
            return;
        Set<byte[]> bssetBytes = new HashSet<byte[]>();
        for (Iterator<T> iterator = sets.iterator(); iterator.hasNext(); ) {
            byte[] bs = SerializeUtil.serialize(iterator.next());
            bssetBytes.add(bs);
        }
        this.ObjectKVData.put(key, bssetBytes);
    }

    @Override
    public <T> Set<T> getObjectSetAll(String key, Class<T> type) {
        if (isKeyExpired(key))
            return null;
        Set<byte[]> bssetBytes = (Set<byte[]>) this.ObjectKVData.get(key);
        if (null == bssetBytes)
            return null;
        Set<T> ret_set = new HashSet<T>();
        for (Iterator<byte[]> iterator = bssetBytes.iterator(); iterator.hasNext(); ) {
            T obj = SerializeUtil.deserialize(iterator.next(), type);
            ret_set.add(obj);
        }
        return ret_set;
    }

    @Override
    public <T> void appendObjectSetMember(String key, T... objects) {
        if (null == objects || objects.length == 0)
            return;
        Set<byte[]> bssetBytes = (Set<byte[]>) this.ObjectKVData.get(key);
        if (null == bssetBytes)
            bssetBytes = new HashSet<byte[]>();
        this.ObjectKVData.put(key, bssetBytes);
        for (T o : objects) {
            byte[] bs = SerializeUtil.serialize(o);
            bssetBytes.add(bs);
        }
    }

    @Override
    public <T> void removeObjectSetMember(String key, T... objects) {
        if (isKeyExpired(key))
            return;
        if (null == objects || objects.length == 0)
            return;
        Set<byte[]> bssetBytes = (Set<byte[]>) this.ObjectKVData.get(key);
        synchronized (bssetBytes) {
            List<byte[]> mark = new ArrayList<byte[]>();
            for (int i = 0; i < objects.length; i++) {
                byte[] valbs = SerializeUtil.serialize(objects[i]);
                for (Iterator<byte[]> iterator = bssetBytes.iterator(); iterator.hasNext(); ) {
                    byte[] bs = iterator.next();
                    if (Arrays.equals(valbs, bs)) {
                        mark.add(bs);
                    }
                }
            }
            for (byte[] bs : mark)
                bssetBytes.remove(bs);
        }
    }

    @Override
    @Deprecated
    public <T> Set<T> getInterObjectSet(Class<T> type, byte[]... keys) {
        throw new RuntimeException("This method is not avalible now!");
    }

    @Override
    public void setStringSet(String key, Set<String> sets) {
        this.ObjectKVData.put(key, sets);
    }

    @Override
    public Set<String> getStringSetAll(String key) {
        if (isKeyExpired(key))
            return null;
        return (Set<String>) this.ObjectKVData.get(key);
    }

    @Override
    public void appendStringSetMember(String key, String... strings) {
        if (null == strings)
            return;
        Set<String> stringSet = (Set<String>) this.ObjectKVData.get(key);
        if (null == stringSet) {
            stringSet = new HashSet<String>();
            this.ObjectKVData.put(key, stringSet);
        }
        for (String string : strings)
            stringSet.add(string);
    }

    @Override
    public void removeStringSetMember(String key, String... strings) {
        if (isKeyExpired(key))
            return;
        if (null == strings || strings.length == 0)
            return;
        Set<String> stringSet = (Set<String>) this.ObjectKVData.get(key);
        synchronized (stringSet) {
            List<String> mark = new ArrayList<String>();
            for (int i = 0; i < strings.length; i++) {
                for (Iterator<String> iterator = stringSet.iterator(); iterator.hasNext(); ) {
                    String s = iterator.next();
                    if ((null == strings[i] && strings[i] == s) || (null != strings[i] && strings[i].equals(s))) {
                        mark.add(s);
                    }
                }
            }
            for (String bs : mark)
                stringSet.remove(bs);
        }
    }

    @Override
    public boolean existsKey(String key) {
        if (isKeyExpired(key))
            return false;
        return this.ObjectKVData.containsKey(key) || this.hashData.containsKey(key);
    }

    @Override
    public long getSetSize(String key) {
        if (isKeyExpired(key))
            return 0;
        Set<Object> set = (Set<Object>) this.ObjectKVData.get(key);
        return null == set ? 0 : set.size();
    }

    ////////////////////////////////////////////   SortedObjectSet

    @Override
    public <T> void setSortedObjectSet(String key, Map<Long, T> set) {
        if (null == set || set.isEmpty()) {
            this.del(key);
            return;
        }
        SortedSet<SortedSetBean> sorterset = Collections.synchronizedSortedSet(new TreeSet<SortedSetBean>());
        this.ObjectKVData.put(key, sorterset);
        for (Map.Entry<Long, T> entry : set.entrySet()) {
            SortedSetBean sb = new SortedSetBean(entry.getKey(), entry.getValue());
            sorterset.add(sb);
        }
    }

    @Override
    public void setStringSortedObjectSet(String key, Map<Long, String> set) {
        if (null == set || set.isEmpty()) {
            this.del(key);
            return;
        }
        SortedSet<SortedSetBean> sorterset = Collections.synchronizedSortedSet(new TreeSet<SortedSetBean>());
        this.ObjectKVData.put(key, sorterset);
        for (Map.Entry<Long, String> entry : set.entrySet()) {
            SortedSetBean sb = new SortedSetBean(entry.getKey(), entry.getValue());
            sorterset.add(sb);
        }
    }

    @Override
    public <T> void appendSortedObjectSetMember(String key, Map<Long, T> set) {
        if (null == set || set.isEmpty()) return;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) {
            storedset = Collections.synchronizedSortedSet(new TreeSet<SortedSetBean>());
            this.ObjectKVData.put(key, storedset);
        }
        for (Map.Entry<Long, T> entry : set.entrySet()) {
            SortedSetBean sb = new SortedSetBean(entry.getKey(), entry.getValue());
            storedset.add(sb);
        }
    }

    @Override
    public void appendStringSortedObjectSetMember(String key,
                                                  Map<Long, String> set) {
        if (null == set || set.isEmpty()) return;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) {
            storedset = Collections.synchronizedSortedSet(new TreeSet<SortedSetBean>());
            this.ObjectKVData.put(key, storedset);
        }
        for (Map.Entry<Long, String> entry : set.entrySet()) {
            SortedSetBean sb = new SortedSetBean(entry.getKey(), entry.getValue());
            storedset.add(sb);
        }
    }

    @Override
    public <T> void removeSortedObjectSetMember(String key, T... members) {
        if (isKeyExpired(key))
            return;
        if (null == members || members.length == 0) return;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset || storedset.isEmpty()) return;
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            Object obj = it.next().getMember();
            byte[] objBytes = null;
            if (null != obj)
                objBytes = SerializeUtil.serialize(obj);
            for (T member : members) {
                if (member == obj && obj == null) {
                    it.remove();
                    break;
                }
                if (null == member || null == obj) {
                    continue;
                } else {
                    byte[] memberBytes = SerializeUtil.serialize(member);
                    if (byteEqual(memberBytes, objBytes)) {
                        it.remove();
                        break;
                    }
                }
            }

        }
    }

    @Override
    public void removeStringSortedObjectSetMember(String key, String... members) {
        if (isKeyExpired(key))
            return;
        if (null == members || members.length == 0) return;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset || storedset.isEmpty()) return;
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            String obj = (String) it.next().getMember();
            for (String member : members) {
                if (member == obj && obj == null) {
                    it.remove();
                    break;
                }
                if (null == member || null == obj) {
                    continue;
                } else {
                    if (obj.equals(member)) {
                        it.remove();
                        break;
                    }
                }
            }

        }
    }

    @Override
    public <T> void removeSortedObjectSetMember(String key, long start, long end) {
        if (isKeyExpired(key))
            return;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) return;
        if (start >= end && end != -1) return;
        if (start >= storedset.size())
            return;
        if (start < 0) start = 0;
        if (end > storedset.size())
            end = storedset.size();
        if (end == -1) end = storedset.size();
        long iter_count = 0;
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            it.next();
            if (iter_count >= start && iter_count < end)
                it.remove();
            iter_count++;
        }
    }

    @Override
    public void removeStringSortedObjectSetMember(String key, long start,
                                                  long end) {
        if (isKeyExpired(key))
            return;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) return;
        if (start >= end && end != -1) return;
        if (start >= storedset.size())
            return;
        if (start < 0) start = 0;
        if (end > storedset.size())
            end = storedset.size();
        if (end == -1) end = storedset.size();
        long iter_count = 0;
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            it.next();
            if (iter_count >= start && iter_count < end)
                it.remove();
            iter_count++;
        }
    }

    @Override
    public <T> Long increSortedObjectSetMemberScore(String key, T member,
                                                    long incr) {
        if (null == key || null == member) return 0L;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) {
            storedset = Collections.synchronizedSortedSet(new TreeSet<SortedSetBean>());
            this.ObjectKVData.put(key, storedset);
        }
        byte[] memberBytes = SerializeUtil.serialize(member);
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            SortedSetBean sb = it.next();
            Object iterMember = sb.getMember();
            byte[] iterMemberBytes = SerializeUtil.serialize(iterMember);
            if (byteEqual(memberBytes, iterMemberBytes)) {
                sb.setScore(sb.getScore() + incr);
                it.remove();
                storedset.add(sb);
                return sb.getScore();
            }
        }

        Map<Long, Object> map = new HashMap<Long, Object>();
        map.put(incr, member);
        this.appendSortedObjectSetMember(key, map);
        return incr;
    }

    @Override
    public Long increStringSortedObjectSetMemberScore(String key,
                                                      String member, long incr) {
        if (null == key || null == member) return 0L;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) {
            storedset = Collections.synchronizedSortedSet(new TreeSet<SortedSetBean>());
            this.ObjectKVData.put(key, storedset);
        }
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            SortedSetBean sb = it.next();
            String iterMember = (String) sb.getMember();
            if (member.equals(iterMember)) {
                sb.setScore(sb.getScore() + incr);
                it.remove();
                storedset.add(sb);
                return sb.getScore();
            }
        }

        Map<Long, Object> map = new HashMap<Long, Object>();
        map.put(incr, member);
        this.appendSortedObjectSetMember(key, map);
        return incr;
    }

    @Override
    public <T> List<T> getSortedObjectSetMember(String key, long start, long end, Class<T> type) {
        if (isKeyExpired(key))
            return null;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) return null;
        if (start >= end && end != -1) return null;
        if (start >= storedset.size())
            return null;
        if (start < 0) start = 0;
        if (end > storedset.size())
            end = storedset.size();
        if (end == -1) end = storedset.size();
        int maxfetch = 0;
        List<T> tmpstoredset = new ArrayList<T>();
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            SortedSetBean sb = it.next();
            if (maxfetch >= start && maxfetch < end)
                tmpstoredset.add((T) sb.getMember());
            maxfetch++;
            if (maxfetch >= end) break;
        }
        return tmpstoredset;
    }

    @Override
    public <T> List<T> getReverseSortedObjectSetMember(String key, long start,
                                                       long end, Class<T> type) {
        if (isKeyExpired(key))
            return null;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) return null;
        if (start >= end && end != -1) return null;
        if (start >= storedset.size())
            return null;
        if (start < 0) start = 0;
        if (end > storedset.size())
            end = storedset.size();
        if (end == -1) end = storedset.size();
        int maxfetch = 0;
        long tmp = start;
        start = storedset.size() - end;
        end = storedset.size() - tmp;
        //end as start and start as end
        List<T> tmpstoredset = new ArrayList<T>();
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            SortedSetBean sb = it.next();
            if (maxfetch >= start && maxfetch < end)
                tmpstoredset.add(0, (T) sb.getMember());
            maxfetch++;
            if (maxfetch >= end) break;
        }
        return tmpstoredset;
    }

    @Override
    public List<String> getReverseStringSortedObjectSetMember(String key,
                                                              long start, long end) {
        if (isKeyExpired(key))
            return null;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) return null;
        if (start >= end && end != -1) return null;
        if (start >= storedset.size())
            return null;
        if (start < 0) start = 0;
        if (end > storedset.size())
            end = storedset.size();
        if (end == -1) end = storedset.size();
        int maxfetch = 0;
        long tmp = start;
        start = storedset.size() - end;
        end = storedset.size() - tmp;
        //end as start and start as end
        List<String> tmpstoredset = new ArrayList<String>();
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            SortedSetBean sb = it.next();
            if (maxfetch >= start && maxfetch < end)
                tmpstoredset.add(0, (String) sb.getMember());
            maxfetch++;
            if (maxfetch >= end) break;
        }
        return tmpstoredset;
    }


    @Override
    public List<String> getStringSortedObjectSetMember(String key, long start,
                                                       long end) {
        if (isKeyExpired(key))
            return null;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        if (null == storedset) return null;
        if (start >= end && end != -1) return null;
        if (start >= storedset.size())
            return null;
        if (start < 0) start = 0;
        if (end > storedset.size())
            end = storedset.size();
        if (end == -1) end = storedset.size();
        int maxfetch = 0;
        List<String> tmpstoredset = new ArrayList<String>();
        for (Iterator<SortedSetBean> it = storedset.iterator(); it.hasNext(); ) {
            tmpstoredset.add((String) it.next().getMember());
            maxfetch++;
            if (maxfetch == (end - start)) break;
        }
        return tmpstoredset;
    }

    public long getSortedSetSize(String key) {
        if (isKeyExpired(key))
            return 0;
        SortedSet<SortedSetBean> storedset = (SortedSet<SortedSetBean>) this.ObjectKVData.get(key);
        return null == storedset ? 0 : storedset.size();
    }

    @Override
    public void del(String key) {
        this.ObjectKVData.remove(key);
        this.hashData.remove(key);
        this.expiredKeyMap.remove(key);
    }

    @Override
    public void hdel(String key, String field) {
        if (isKeyExpired(key))
            return;
        Map<Object, Object> map = this.hashData.get(key);
        if (null == map)
            return;
        map.remove(key);
    }

    @Override
    public void hdel(String key, Object field) {
        assertKey(key);
        Map<Object, Object> itemMap = hashData.get(key);
        if (null == itemMap) {
            return;
        }
        byte[] fieldBytes = SerializeUtil.serialize(field);
        assertKey(fieldBytes);
        String fieldMd5 = Md5Util.getMd5(fieldBytes);
        itemMap.remove(fieldMd5);
    }


    @Override
    public void incr(String key, Long value) {
        if (null == value)
            return;
        String _stored = (String) ObjectKVData.get(key);
        if (null == _stored) {
            _stored = "0";
        }
        if (isKeyExpired(key))
            _stored = "0";
        if (!_stored.matches("[0-9]+"))
            throw new IllegalStateException("Target type is not a number.");
        Long stored = Long.valueOf(_stored);
        this.ObjectKVData.put(key, String.valueOf(stored + value));
    }

    @Override
    public void hincr(String key, String field, Long value) {
        if (null == value)
            return;
        Map<Object, Object> map = this.hashData.get(key);
        if (null == map) {
            map = new HashMap<Object, Object>();
            map.put(field, value);
        } else {
            String _stored = (String) map.get(field);
            if (_stored == null)
                _stored = "0";
            if (isKeyExpired(key))
                _stored = "0";
            if (!_stored.matches("[0-9]+"))
                throw new IllegalStateException("Target type is not a number.");
            Long store = Long.valueOf(String.valueOf(_stored));
            map.put(field, (store + value));
        }
    }


    /**
     * 检查key是否过期
     *
     * @param key
     * @return
     */
    private boolean isKeyExpired(String key) {
        Long expiredTime = expiredKeyMap.get(key);
        if (null == expiredTime)
            return false;
        return System.currentTimeMillis() > expiredTime ? true : false;
    }

    public String info() {
        return "{\"keys\":\"" + (ObjectKVData.size() + hashData.size()) + "\"}";
    }

    /**
     * 定时器任务，定时清理过期key
     *
     * @author Hetianyi
     */
    class CleanKeyTask extends TimerTask {
        int cleancount;

        @Override
        public void run() {
            logger.debug("Cleaning expired keys...");
            cleancount = 0;
            String key = null;
            String okey = null;
            Long oval = null;
            synchronized (expiredKeyMap) {
                for (Iterator<String> outerIter = expiredKeyMap.keySet().iterator(); outerIter.hasNext(); ) {
                    okey = outerIter.next();
                    oval = expiredKeyMap.get(okey);
                    if (System.currentTimeMillis() > oval) {
                        ObjectKVData.remove(okey);
                        hashData.remove(okey);
                        outerIter.remove();
                        cleancount++;
                        logger.debug("del key -> {}", key);
                    }

                }
            }
            logger.debug("Clean total keys : {}", cleancount);
        }
    }

    @Override
    public void exKey(String key, int sec) {
        if (sec > 0) {
            expiredKeyMap.put(key, System.currentTimeMillis() + 1000 * sec);
        }
    }

    @Override
    public Set<String> keys(String pattern) {
        Set<String> keys = new HashSet<String>();
        for (Iterator<String> it = ObjectKVData.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            if (!isKeyExpired(key) && key.matches(pattern)) {
                keys.add(key);
            }
        }
        for (Iterator<String> it = hashData.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            if (!isKeyExpired(key) && key.matches(pattern)) {
                keys.add(key);
            }
        }
        return keys;
    }

}
