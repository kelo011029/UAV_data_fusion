package com.telecom;

public class AOABean {
    private String devId;           // 设备 ID
    private String batchno;         // 批次号
    private double distance;        // 距离
    private int lasting_time;       // 持续时间
    private double channel;         // 信道
    private String model;           // 设备型号
    private double direction;       // 方向
    private long timestamp;         // 时间戳

    // Getter 和 Setter 方法
    public String getDevId() {
        return devId;
    }

    public void setDevId(String devId) {
        this.devId = devId;
    }

    public String getBatchno() {
        return batchno;
    }

    public void setBatchno(String batchno) {
        this.batchno = batchno;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public int getLasting_time() {
        return lasting_time;
    }

    public void setLasting_time(int lasting_time) {
        this.lasting_time = lasting_time;
    }

    public double getChannel() {
        return channel;
    }

    public void setChannel(double channel) {
        this.channel = channel;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public double getDirection() {
        return direction;
    }

    public void setDirection(double direction) {
        this.direction = direction;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // toString 方法，用于打印对象信息
    @Override
    public String toString() {
        return "AOABean{" +
                "devId='" + devId + '\'' +
                ", batchno='" + batchno + '\'' +
                ", distance=" + distance +
                ", lasting_time=" + lasting_time +
                ", channel=" + channel +
                ", model='" + model + '\'' +
                ", direction=" + direction +
                ", timestamp=" + timestamp +
                '}';
    }
}
