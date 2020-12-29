package org.learn.flink.connector.kafka;

public class TypeRecord {
    //小男孩按键盘比赛游戏
    String boyName;
    char key;

    TypeRecord(String boyName, char key) {
        this.boyName = boyName;
        this.key = key;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", boyName, key);
    }
}
