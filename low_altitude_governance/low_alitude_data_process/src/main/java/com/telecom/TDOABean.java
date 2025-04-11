package com.telecom;
public class TDOABean {
    private double gpsLongitude;
    private double gpsLatitude;
    private double longitude;
    private double latitude;
    private String lastCoorDate;
    private double frequency;
    private double height;
    private double speed;
    private int lastingTime;

    // 无参构造函数
    public TDOABean() {
    }

    // Getters and Setters

    public double getGpsLongitude() {
        return gpsLongitude;
    }

    public void setGpsLongitude(double gpsLongitude) {
        this.gpsLongitude = gpsLongitude;
    }

    public double getGpsLatitude() {
        return gpsLatitude;
    }

    public void setGpsLatitude(double gpsLatitude) {
        this.gpsLatitude = gpsLatitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String getLastCoorDate() {
        return lastCoorDate;
    }

    public void setLastCoorDate(String lastCoorDate) {
        this.lastCoorDate = lastCoorDate;
    }

    public double getFrequency() {
        return frequency;
    }

    public void setFrequency(double frequency) {
        this.frequency = frequency;
    }

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getLastingTime() {
        return lastingTime;
    }

    public void setLastingTime(int lastingTime) {
        this.lastingTime = lastingTime;
    }

    // toString 方法，用于打印对象信息
    @Override
    public String toString() {
        return "TDOAData{" +
                "gpsLongitude=" + gpsLongitude +
                ", gpsLatitude=" + gpsLatitude +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", lastCoorDate='" + lastCoorDate + '\'' +
                ", frequency=" + frequency +
                ", height=" + height +
                ", speed=" + speed +
                ", lastingTime=" + lastingTime +
                '}';
    }
}