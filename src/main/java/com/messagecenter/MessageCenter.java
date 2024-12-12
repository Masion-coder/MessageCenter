package com.messagecenter;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.Date;
import java.util.HashMap;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageCenter {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int capacity = 1000000;
        ArrayBlockingQueue<Message> buffer = new ArrayBlockingQueue<>(capacity);

        Map<String, List<Observer>> subscribe = new HashMap<>();

        LongAdder index = new LongAdder();

        MySQL mySQL = new MySQL("root", "123456");
        mySQL.setDatabase("messagecenter");
        mySQL.connect();

        ResultSet rs = mySQL.query(capacity);
        while (rs.next()) {
            Long serialNumber = rs.getLong("serialNumber");
            Date time = rs.getDate("time");
            String tag = rs.getString("tag");
            Object value = MAPPER.readValue(rs.getString("value"), new TypeReference<Object>() {
            });
            buffer.add(new Message(serialNumber, time, tag, value));
        }

        class Task extends TimerTask {
            @Override
            public void run() {
                List<Message> set = new LinkedList<>();

                long max = index.longValue();
                for (Message message : List.copyOf(buffer)) {
                    if (message.serialNumber > index.longValue()) {
                        max = message.serialNumber;
                        set.add(message);
                    }
                }

                index.reset();

                index.add(max);

                System.out.println("buffer:" + buffer.size());
                System.out.println("index:" + index);

                try {
                    System.out.println("开始备份");
                    int n = mySQL.insert(set);
                    System.out.println("备份成功:" + n);
                } catch (Exception e) {
                    System.out.println("备份失败");
                    System.out.println("ERROE:" + e.getMessage());
                }
                System.out.println();
            }
        }

        index.add(mySQL.getSerialNumber());
        Pull.m_adder.add(mySQL.getSerialNumber());

        Timer timer = new Timer();
        timer.schedule(new Task(), 10000, 10000);

        Pull pull = new Pull(buffer, subscribe);

        pull.setMode(Pull.BROADCAST_MODE);

        new Thread(pull).start();
        new Thread(new Push(buffer)).start();
        new Thread(new Subscribe(buffer, subscribe)).start();
    }
}