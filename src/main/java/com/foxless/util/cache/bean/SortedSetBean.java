package com.foxless.util.cache.bean;

/**
 * 模仿redis的SortedSet创建的有序集合
 *
 * @author Hetianyi 2017/12/30
 * @version 1.0
 */
public class SortedSetBean implements Comparable<SortedSetBean> {

    //因子
    private long score;
    //SortedSet成员
    private Object member;

    public SortedSetBean(long score, Object member) {
        this.score = score;
        this.member = member;
    }

    @Override
    public int compareTo(SortedSetBean o) {
        return this.score > o.getScore() ? 1 : -1;
    }

    public long getScore() {
        return score;
    }

    public void setScore(long score) {
        this.score = score;
    }

    public Object getMember() {
        return member;
    }

    public void setMember(Object member) {
        this.member = member;
    }
}
