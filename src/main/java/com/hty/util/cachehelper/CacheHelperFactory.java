/**  
* @Title: JedisHelperFactory.java
* @Package com.hty.util.jedis
* @Description: TODO
* @author liugang  
* @date 2017年3月13日 下午4:26:43 
*/
package com.hty.util.cachehelper;

import java.lang.reflect.Proxy;

import com.hty.util.cachehelper.aop.JedisHelperInvocationHandler;
import com.hty.util.cachehelper.cacher.JedisCacheHelper;
import com.hty.util.cachehelper.cacher.LocalCacheHelper;
import com.hty.util.cachehelper.cacher.impl.LocalCacheHelperImpl;

/**
 * @ClassName: JedisHelperFactory
 * @Description: 
 * @author liugang
 * @date 2017年3月13日 下午4:26:43
 */
public class CacheHelperFactory  {
	private static JedisCacheHelper jedisHelper;
	private CacheHelperFactory() {
	}
	/**
	 * 产生一个代理JedisHelper类
	 */
	public static final JedisCacheHelper getJedisCacheHelper() {
		if(null == jedisHelper) {
			JedisHelperInvocationHandler jedisHelperInvocationHandler =
					new JedisHelperInvocationHandler();
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
