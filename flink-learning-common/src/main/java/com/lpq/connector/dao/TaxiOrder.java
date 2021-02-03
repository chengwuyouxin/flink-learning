package com.lpq.connector.dao;

/**
 * @author liupengqiang
 * @date 2020/6/5
 */
public class TaxiOrder {
    private Long OrderTime;
    private int driverId;
    private String driverSex;
    private int passengerId;
    private String passengerSex;
    private int isCancel;

    public int getDriverId() {
        return driverId;
    }

    public void setDriverId(int driverId) {
        this.driverId = driverId;
    }

    public int getPassengerId() {
        return passengerId;
    }

    public void setPassengerId(int passengerId) {
        this.passengerId = passengerId;
    }

    public Long getOrderTime() {
        return OrderTime;
    }

    public void setOrderTime(Long orderTime) {
        OrderTime = orderTime;
    }

    public String getDriverSex() {
        return driverSex;
    }

    public void setDriverSex(String driverSex) {
        this.driverSex = driverSex;
    }

    public String getPassengerSex() {
        return passengerSex;
    }

    public void setPassengerSex(String passengerSex) {
        this.passengerSex = passengerSex;
    }

    public int getIsCancel() {
        return isCancel;
    }

    public void setIsCancel(int isCancel) {
        this.isCancel = isCancel;
    }

    public TaxiOrder() {
    }

    public TaxiOrder(Long orderTime, int driverId, String driverSex, int passengerId, String passengerSex, int isCancel) {
        OrderTime = orderTime;
        this.driverId = driverId;
        this.driverSex = driverSex;
        this.passengerId = passengerId;
        this.passengerSex = passengerSex;
        this.isCancel = isCancel;
    }

    @Override
    public String toString() {
        return "TaxiOrder{" +
                "OrderTime=" + OrderTime +
                ", driverId=" + driverId +
                ", driverSex='" + driverSex + '\'' +
                ", passengerId=" + passengerId +
                ", passengerSex='" + passengerSex + '\'' +
                ", isCancel=" + isCancel +
                '}';
    }
}
