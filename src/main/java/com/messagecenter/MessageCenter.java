package com.messagecenter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageCenter {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ArrayBlockingQueue<Message> buffer = new ArrayBlockingQueue<>(100000);
        
        List<Observer> subscribe = new LinkedList<>();

        Long adder = Long.valueOf(0);

        if (new File("data.json").isFile()) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(new File("data.json")), "UTF-16LE"))) {
                char[] buff = new char[1024];
                String json = "";

                do {
                    int len = br.read(buff);
                    if (len == -1)
                        break;
                    json += String.copyValueOf(buff).substring(0, len);
                } while (br.ready());
                // System.out.println("json:" + json);

                Data data = MAPPER.readValue(json, new TypeReference<Data>() {
                });

                for (Message message : data.messages) {
                    if (message.serialNumber > adder) {
                        adder = message.serialNumber;
                    }
                    buffer.add(message);
                }
                // System.out.println("buffer:" + buffer.size());
            } catch (Exception e) {
                System.out.println("ERROE(main):" + e.getMessage());
                if (new File("data.json").exists())
                    new File("data.json").delete();
            }
        }

        class Task extends TimerTask {
            @Override
            public void run() {
                try (BufferedWriter bw = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(new File("data.json")), "UTF-16LE"))) {
                    bw.write(MAPPER.writeValueAsString(new Data(List.copyOf(buffer))));
                    System.out.println("buffer:" + List.copyOf(buffer).size());
                } catch (Exception e) {
                    System.out.println("ERROE:" + e.getMessage());
                }
            }
        }

        Timer timer = new Timer();
        timer.schedule(new Task(), 0, 10000);


        Pull.m_adder.add(adder);
        new Thread(new Pull(buffer, subscribe)).start();
        new Thread(new Push(buffer)).start();
        new Thread(new Subscribe(subscribe)).start();
    }
}