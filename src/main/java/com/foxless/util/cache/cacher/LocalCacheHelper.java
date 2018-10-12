package com.foxless.util.cache.cacher;



public interface LocalCacheHelper extends CacheHelper{
	
	void hset(String key, Object field, Object value);

	String info();
}