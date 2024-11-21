package com.messagecenter;

public class Message {

    public long serialNumber;
    public long time;
    public String tag;
    public Object value;
    public Message() {
    }

    public Message(int serialNumber, long time, String tag, Object value) {
        this.serialNumber = serialNumber;
        this.time = time;
        this.tag = tag;
        this.value = value;
    }
}
