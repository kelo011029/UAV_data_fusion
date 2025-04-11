package com.telecom;
/*
这是老的jm数据结构，具体数据如下：
帮忙根据这个数据生成一个pojo类，类名：JMBeanOld，要求字段名称不能变
"intrusion_start_time": "11022910\u318d20241114100126790",
"latitude": 30.248329165972358,
"target_type": 1,
"droneUK": "5cd108e5-1431-4bd5-a1a2-448b2c620a52211",
"batch_num": "11022910\u318d2024111410031666400",
"speed": 0.4700184464454651,
"frequency": 1.434949278831482,
"driver_longitude": 120.23461151123047,
"lasting_time": 313,
"data_type": 1,
"model": "Haikang_1400",
"driver_latitude": 30.248483657836914,
"longitude": 120.23422242077874,
"height": 74.580322265625,
"timestamp": 1731550202184}
*/

public class JMBeanOld {
    //目标第一次被侦测到的毫秒级时间（例如：20240813132319495）
    private String intrusion_start_time;
    //纬度（wgs84坐标系）
    private double latitude;
    //
    private int target_type;
    //唯一标识符，代表同一架次
    private String droneUK;
    //当前目标的批次的上报时间（例如：20240813132319）
    private String batch_num;
    //速度
    private double speed;
    //频段
    private double frequency;
    //飞手经度（wgs84坐标系）
    private double driver_longitude;
    //持续时间
    private int lasting_time;
    //
    private int data_type;
    //型号
    private String model;
    //飞手纬度（wgs84坐标系）
    private double driver_latitude;
    //经度（wgs84坐标系）
    private double longitude;
    //高度
    private double height;
    //当前时间戳，13位
    private long timestamp;

    // Getters
    public String getIntrusion_start_time() {
        return intrusion_start_time;
    }

    public double getLatitude() {
        return latitude;
    }

    public int getTarget_type() {
        return target_type;
    }

    public String getDroneUK() {
        return droneUK;
    }

    public String getBatch_num() {
        return batch_num;
    }

    public double getSpeed() {
        return speed;
    }

    public double getFrequency() {
        return frequency;
    }

    public double getDriver_longitude() {
        return driver_longitude;
    }

    public int getLasting_time() {
        return lasting_time;
    }

    public int getDataType() {
        return data_type;
    }

    public String getModel() {
        return model;
    }

    public double getDriver_latitude() {
        return driver_latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getHeight() {
        return height;
    }

    public long getTimestamp() {
        return timestamp;
    }

    // Setters
    public void setIntrusion_start_time(String intrusion_start_time) {
        this.intrusion_start_time = intrusion_start_time;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setTarget_type(int target_type) {
        this.target_type = target_type;
    }

    public void setDroneUK(String droneUK) {
        this.droneUK = droneUK;
    }

    public void setBatch_num(String batch_num) {
        this.batch_num = batch_num;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public void setFrequency(double frequency) {
        this.frequency = frequency;
    }

    public void setDriver_longitude(double driver_longitude) {
        this.driver_longitude = driver_longitude;
    }

    public void setLasting_time(int lasting_time) {
        this.lasting_time = lasting_time;
    }

    public void setDataType(int data_type) {
        this.data_type = data_type;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setDriver_latitude(double driver_latitude) {
        this.driver_latitude = driver_latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // toString method for debugging
    @Override
    public String toString() {
        return "JMBeanOld{" +
                "intrusion_start_time='" + intrusion_start_time + '\'' +
                ", latitude=" + latitude +
                ", target_type=" + target_type +
                ", droneUK='" + droneUK + '\'' +
                ", batch_num='" + batch_num + '\'' +
                ", speed=" + speed +
                ", frequency=" + frequency +
                ", driver_longitude=" + driver_longitude +
                ", lasting_time=" + lasting_time +
                ", data_type=" + data_type +
                ", model='" + model + '\'' +
                ", driver_latitude=" + driver_latitude +
                ", longitude=" + longitude +
                ", height=" + height +
                ", timestamp=" + timestamp +
                '}';
    }
}


