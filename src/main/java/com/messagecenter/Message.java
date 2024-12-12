package com.messagecenter;

import java.util.Date;

public class Message {

    public long serialNumber;
    public Date time;
    public String tag;
    public Object value;
    public Message() {
    }

    public Message(long serialNumber, Date time, String tag, Object value) {
        this.serialNumber = serialNumber;
        this.time = time;
        this.tag = tag;
        this.value = value;
    }
}
