package com.hty.util.cachehelper;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.runtime.DefaultIdStrategy;
import com.dyuproject.protostuff.runtime.Delegate;
import com.dyuproject.protostuff.runtime.RuntimeEnv;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.hty.util.cachehelper.bean.DataContainer;
import com.hty.util.cachehelper.bean.TimestampDelegate;


@SuppressWarnings("unchecked")
public class SerializeUtil {

    //用于缓存Protostuff的Schema信息
    private static final Map<Class<?>, RuntimeSchema<?>> schemas =
            new HashMap<Class<?>, RuntimeSchema<?>>();

    public static final int LINK_BUFFER_SIZE = 256;


    /**
     * 时间戳转换Delegate，解决时间戳转换后错误问题
     */
    private final static Delegate<Timestamp> TIMESTAMP_DELEGATE = new TimestampDelegate();

    private final static DefaultIdStrategy idStrategy = ((DefaultIdStrategy) RuntimeEnv.ID_STRATEGY);

    static {
        idStrategy.registerDelegate(TIMESTAMP_DELEGATE);
    }


    /**
     * 从schemas集合获取类的RuntimeSchema，如果不存在，则创建，加入schemas并返回
     */
    public static <T> RuntimeSchema<T> getSchema(Class<T> type) {
        RuntimeSchema<T> schema = (RuntimeSchema<T>) schemas.get(type);
        if (null == schema) {
            schema = RuntimeSchema.createFrom(type, idStrategy);
            schemas.put(type, schema);
        }
        return schema;
    }

    /**
     * 将一个对象反序列化为字节数组
     */
    public static byte[] serialize(Object obj) {
        DataContainer objContainer = new DataContainer(obj);
        RuntimeSchema<DataContainer> schema = getSchema(DataContainer.class);
        LinkedBuffer buffer = LinkedBuffer.allocate(LINK_BUFFER_SIZE);
        byte[] obj_bs = ProtostuffIOUtil.toByteArray(objContainer, schema, buffer);
        buffer.clear();
        buffer = null;
        return obj_bs;
    }

    /**
     * 将字节数组序列化为指定类型的类
     */
    public static <T> T deserialize(byte[] obj_bs, Class<T> type) {
        DataContainer valueContainer = new DataContainer();
        RuntimeSchema<DataContainer> schema = getSchema(DataContainer.class);
        if (null != obj_bs && obj_bs.length > 0)
            ProtostuffIOUtil.mergeFrom(obj_bs, valueContainer, schema);
        return (T) valueContainer.getData();
    }
}
