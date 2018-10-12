/**
 * @author Hetianyi
 * @date 2018/5/5
 */
package com.foxless.util.cache;

import java.lang.reflect.Proxy;

import com.foxless.util.cache.cacher.LocalCacheHelper;
import com.foxless.util.cache.aop.JedisHelperInvocationHandler;
import com.foxless.util.cache.bean.JedisConfigBean;
import com.foxless.util.cache.cacher.JedisCacheHelper;
import com.foxless.util.cache.cacher.impl.LocalCacheHelperImpl;


public class CacheHelperFactory {

    private static JedisCacheHelper jedisHelper;

    private CacheHelperFactory() {
    }

    /**
     * 产生一个代理JedisHelper类
     */
    public static final JedisCacheHelper getJedisCacheHelper(JedisConfigBean jedisConfigBean) {
        if (null == jedisHelper) {
            JedisHelperInvocationHandler jedisHelperInvocationHandler =
                    new JedisHelperInvocationHandler(jedisConfigBean);
            jedisHelper = (JedisCacheHelper) Proxy.newProxyInstance(JedisCacheHelper.class.getClassLoader(),
                    new Class[]{JedisCacheHelper.class},
                    jedisHelperInvocationHandler);
        }
        return jedisHelper;
    }

    /**
     * 产生一个LocalCacheHelper
     */
    public static final LocalCacheHelper getLocalCacheHelper() {
        return LocalCacheHelperImpl.getInstance();
    }
}
