package com.messagecenter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Observer implements Runnable {
    private String m_name;
    private Socket m_socket;
    private BlockingQueue<Message> m_buffer;
    private List<String> m_tags = new LinkedList<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public Observer(Socket socket, BlockingQueue<Message> buffer) {
        m_socket = socket;
        m_buffer = buffer;
    }

    public String getName() {
        return m_name;
    }

    public Socket getSocket() {
        return m_socket;
    }

    public BlockingQueue<Message> getBuffer() {
        return m_buffer;
    }

    public List<String> getTags() {
        return m_tags;
    }
    
    public void setName(String name) {
        m_name = name;
    }

    public void setSocket(Socket socket) {
        m_socket = socket;
    }

    public void setBuffer(BlockingQueue<Message> buffer) {
        m_buffer = buffer;
    }

    public boolean offer(Message message) {
        return m_buffer.offer(message);
    }

    public void put(Message message) throws InterruptedException {
        m_buffer.put(message);
    }

    public void setTags(List<String> tags) {
        m_tags = tags;
    }

    @Override
    public void run() {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(m_socket.getInputStream(), "UTF-16LE"));
                BufferedWriter bw = new BufferedWriter(
                        new OutputStreamWriter(m_socket.getOutputStream(), "UTF-16LE"))) {
            m_socket.setSoTimeout(2000);

            char[] buff = new char[4096];
            String json = "";
            do {
                int len = br.read(buff);
                if (len == -1)
                    continue;
                json += String.copyValueOf(buff).substring(0, len);
                Thread.sleep(10);
            } while (br.ready());

            // System.out.println("json:" + json);

            Map<String, Object> objectMap = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
            });

            

            if (objectMap.containsKey("name")) {
                m_name = objectMap.get("name").toString();
            }
            if (objectMap.containsKey("tags")) {
                for (String tag : (ArrayList<String>)objectMap.get("tags")) {
                    // System.out.println("test-1(Observer):" + tag);
                    m_tags.add(tag);
                }
            }


            while (!m_socket.isClosed()) {
                bw.write(MAPPER.writeValueAsString(m_buffer.take()) + ',');
                bw.flush();
                // System.out.println("test2(Observer)");
            }
        } catch (Exception e) {
            System.out.println("ERROE(Observer):" + e.getMessage());
        }
    }
}
