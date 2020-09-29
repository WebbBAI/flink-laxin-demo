package com.homework.flink.step1;

import java.util.Objects;

public class TestBean1 {
    int event_id;
    String unionid;
    int act_id;
    int channel_id;
    long event_time;

    public TestBean1(int event_id, String unionid, int act_id, int channel_id, long event_time) {
        this.event_id = event_id;
        this.unionid = unionid;
        this.act_id = act_id;
        this.channel_id = channel_id;
        this.event_time = event_time;
    }

    public TestBean1() {
    }

    public int getEvent_id() {
        return event_id;
    }

    public void setEvent_id(int event_id) {
        this.event_id = event_id;
    }

    public String getUnionid() {
        return unionid;
    }

    public void setUnionid(String unionid) {
        this.unionid = unionid;
    }

    public int getAct_id() {
        return act_id;
    }

    public void setAct_id(int act_id) {
        this.act_id = act_id;
    }

    public int getChannel_id() {
        return channel_id;
    }

    public void setChannel_id(int channel_id) {
        this.channel_id = channel_id;
    }

    public long getEvent_time() {
        return event_time;
    }

    public void setEvent_time(long event_time) {
        this.event_time = event_time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestBean1 testBean1 = (TestBean1) o;
        return event_id == testBean1.event_id &&
                act_id == testBean1.act_id &&
                channel_id == testBean1.channel_id &&
                event_time == testBean1.event_time &&
                Objects.equals(unionid, testBean1.unionid);
    }

    @Override
    public int hashCode() {

        return Objects.hash(event_id, unionid, act_id, channel_id, event_time);
    }

    @Override
    public String toString() {
        return "TestBean1{" +
                "event_id=" + event_id +
                ", unionid='" + unionid + '\'' +
                ", act_id=" + act_id +
                ", channel_id=" + channel_id +
                ", event_time=" + event_time +
                '}';
    }
}
