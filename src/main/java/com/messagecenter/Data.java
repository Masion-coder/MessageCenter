package com.messagecenter;

import java.util.List;

public class Data {
    public List<Message> messages;

    public Data() {
    }

    public Data(List<Message> messages) {
        this.messages = messages;
    }

    public boolean add(Message e) {
        return messages.add(e);
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }
}
