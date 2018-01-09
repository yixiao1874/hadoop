package com.gtja.recommendjava;

import java.io.Serializable;

public class Person implements Serializable {
    private int userID;
    private int itemID;
    private double pref;

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public int getItemID() {
        return itemID;
    }

    public void setItemID(int itemID) {
        this.itemID = itemID;
    }

    public double getPref() {
        return pref;
    }

    public void setPref(double pref) {
        this.pref = pref;
    }
}
