package org.learn.flink.connector;

import java.sql.Time;

public class ClickEvent {
    String user;
    Time ctime;
    String url;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Time getCtime() {
        return ctime;
    }

    public void setCtime(Time ctime) {
        this.ctime = ctime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString(){
        return String.format("%s-%s-%s", user,ctime.toString(),url);
    }
}
