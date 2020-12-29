package org.learn.flink;

import java.sql.Time;
import java.sql.Timestamp;

public class ClickEvent {
    String user;
    String time;
    Timestamp timestamp;
    String url;

    public ClickEvent() {

    }

    public ClickEvent(String user, String time, String url) {
        this.user = user;
        this.time = time;
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%s", user, time.toString(), url);
    }
}
