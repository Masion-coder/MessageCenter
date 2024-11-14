package com.messagecenter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageCenter {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ArrayBlockingQueue<Long> buffer = new ArrayBlockingQueue<>(100000);

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

                Map<String, Object> objectMap = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
                });

                if (objectMap.containsKey("data")) {
                    List<Long> data = MAPPER.readValue(objectMap.get("data").toString(),
                            new TypeReference<List<Long>>() {
                            });
                    for (Long num : data) {
                        if (buffer.offer(num) == false) {
                            break;
                        }
                    }
                }
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
                    bw.write("{\"data\":" + buffer.toString() + '}');
                } catch (Exception e) {
                    System.out.println("ERROE:" + e.getMessage());
                }
            }
        }

        Timer timer = new Timer();
        timer.schedule(new Task(), 0, 10000);

        new Thread(new Pull(buffer)).start();
        new Thread(new Push(buffer)).start();
    }
}