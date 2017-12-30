package com.hty.util.cachehelper.util;

import java.util.Comparator;

import redis.clients.jedis.Tuple;

public class SortedSetComparator<T> implements Comparator<T> {
    @Override
    public int compare(T o1, T o2) {
        if (o1.getClass() == Tuple.class) {
            Tuple t1 = (Tuple) o1;
            Tuple t2 = (Tuple) o2;
            return t1.compareTo(t2);
        }
        return 0;
    }
}
