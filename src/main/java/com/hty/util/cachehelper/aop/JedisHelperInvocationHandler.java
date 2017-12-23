/**  
* @Title: JedisHelperInvocationHandler.java
* @Package com.hty.util.jedis.aop
* @Description: TODO
* @author liugang  
* @date 2017年3月13日 下午4:28:49 
*/
package com.hty.util.cachehelper.aop;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.hty.util.cachehelper.cacher.impl.JedisHelperImpl;

/**
 * 用切面来执行JedisHelper的方法，如果方法执行过程中抛出异常，则回滚事务。
 */
public class JedisHelperInvocationHandler implements InvocationHandler {
	private JedisHelperImpl targetObject;
	
	public JedisHelperInvocationHandler() {
		if(null == targetObject)
			targetObject = new JedisHelperImpl();
	}
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		Object returnVal = null;
		try {
			returnVal = method.invoke(targetObject, args);
		} catch (Exception e) {
			Method m3 = JedisHelperImpl.class.getMethod("clear");
			m3.invoke(targetObject);//清除本地线程数据
			throw e;
		}
		return returnVal;
	}

}
