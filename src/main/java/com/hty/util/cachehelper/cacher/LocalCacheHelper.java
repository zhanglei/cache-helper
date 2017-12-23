package com.hty.util.cachehelper.cacher;



public interface LocalCacheHelper extends CacheHelper{
	
	public abstract void hset(String key, Object field, Object value);
	public String info();
}