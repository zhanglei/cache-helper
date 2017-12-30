package com.hty.util.cachehelper.bean;

/**
 * 序列化数据容器
 * @author Hetianyi 2017/12/30
 * @version 1.0
 */
public class DataContainer {

    public DataContainer() {
    }

    private Object data;

    public DataContainer(Object data) {
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
