package com.telecom;

/*
这是新的jm数据结构，具体数据如下：
帮忙根据这个数据生成一个pojo类，类名：JMBean，要求字段名称不能变
{
"batch_num":"92025u318d202411261051373704",
"detect_type":1,
"drone_uuid":"F6Z9C23AR003DES6",
"frequency":5.8,
"height":311.0,
"intrusion_start_time":"92025u318d20241126105137370",
"lasting_time":1716,
"latitude":22.526271000446798,
"longitude":114.07861900031418,
"model":"DJI DJI Mini 4Pro",
"scanID":[{"id":"92025"}],
"speed":0.0,
"type":1,
"station_id":"100094"
}
 */

import java.sql.Timestamp;

public class JMBean {
    //当前目标的批次的上报时间（例如：20240813132319）
    private String batch_num;
    //探测类型（0-多站1-单站）
    private int detect_type;
    //无人机序列号
    private String drone_uuid;
    //频段
    private double frequency;
    //高度
    private double height;
    //目标第一次被侦测到的毫秒级时间（例如：20240813132319495）
    private String intrusion_start_time;
    //持续时间
    private int lasting_time;
    //纬度（wgs84坐标系）
    private double latitude;
    //经度（wgs84坐标系）
    private double longitude;
    //型号
    private String model;
    //侦测设备列表
    private String scanID;
    //速度
    private double speed;
    //类型（0-遥控器1-无人机）
    private int type;
    //站点ID（由杰能科世提供
    private String station_id;
    //时间戳：
    // batch_num转long后 + lasting_time
    private long timestamp;

    private Timestamp ts_ltz;

    // Getters
    public String getBatch_num() {
        return batch_num;
    }

    public int getDetect_type() {
        return detect_type;
    }

    public String getDrone_uuid() {
        return drone_uuid;
    }

    public double getFrequency() {
        return frequency;
    }

    public double getHeight() {
        return height;
    }

    public String getIntrusion_start_time() {
        return intrusion_start_time;
    }

    public int getLasting_time() {
        return lasting_time;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public String getModel() {
        return model;
    }

    public String getScanID() {
        return scanID;
    }

    public double getSpeed() {
        return speed;
    }

    public int getType() {
        return type;
    }

    public String getStation_id() {
        return station_id;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public Timestamp getTs_ltz() {
        return ts_ltz;
    }

    // Setters
    public void setBatch_num(String batch_num) {
        this.batch_num = batch_num;
    }

    public void setDetect_type(int detect_type) {
        this.detect_type = detect_type;
    }

    public void setDrone_uuid(String drone_uuid) {
        this.drone_uuid = drone_uuid;
    }

    public void setFrequency(double frequency) {
        this.frequency = frequency;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public void setIntrusion_start_time(String intrusion_start_time) {
        this.intrusion_start_time = intrusion_start_time;
    }

    public void setLasting_time(int lasting_time) {
        this.lasting_time = lasting_time;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setScanID(String scanID) {
        this.scanID = scanID;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setStation_id(String station_id) {
        this.station_id = station_id;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public void setTs_ltz(Timestamp ts_ltz) {
        this.ts_ltz = ts_ltz;
    }

    // Inner Class for ScanID
    public static class ScanID {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    // toString method for debugging
    @Override
    public String toString() {
        return "JMBean{" +
                "batch_num='" + batch_num + '\'' +
                ", detect_type=" + detect_type +
                ", drone_uuid='" + drone_uuid + '\'' +
                ", frequency=" + frequency +
                ", height=" + height +
                ", intrusion_start_time='" + intrusion_start_time + '\'' +
                ", lasting_time=" + lasting_time +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", model='" + model + '\'' +
                ", scanID=" + scanID +
                ", speed=" + speed +
                ", type=" + type +
                ", station_id='" + station_id + '\'' +
                ", timestamp=" + timestamp +
                ", ts_ltz=" + ts_ltz +
                '}';
    }
}


