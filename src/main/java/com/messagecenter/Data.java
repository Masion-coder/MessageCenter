package com.messagecenter;

import java.util.List;

public class Data {
    public List<Long> data;

    public Data(List<Long> data) {
        this.data = data;
    }

    public boolean add(Long e) {
        return data.add(e);
    }
}
