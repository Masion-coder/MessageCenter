package com.messagecenter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Push implements Runnable {
    private ArrayBlockingQueue<Message> m_buffer;
    private ThreadPoolExecutor m_pool;
    private int m_port = 40000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    class Task implements Runnable {
        private Socket s;

        public Task(Socket s) {
            this.s = s;
        }

        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-16LE"));
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), "UTF-16LE"))) {
                s.setSoTimeout(2000);
                char[] buff = new char[4096];
                String json = "";

                // System.out.println("开始接收参数");

                do {
                    int len = br.read(buff);
                    if (len == -1)
                        continue;
                    json += String.copyValueOf(buff).substring(0, len);
                    Thread.sleep(10);
                } while (br.ready());

                // System.out.println("已接收参数");

                // System.out.println(json);

                Map<String, Object> objectMap = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
                });

                if (objectMap.containsKey("n")) {
                    int n = MAPPER.readValue(objectMap.get("n").toString(), new TypeReference<Integer>() {
                    });
                    Data data = new Data(new ArrayList<>(n));
                    while (n-- != 0) {
                        Message temp = m_buffer.poll();
                        if (temp == null)
                            break;
                        data.add(temp);
                    }
                    // System.out.println("发送:" + MAPPER.writeValueAsString(data));
                    bw.write(MAPPER.writeValueAsString(data));
                    bw.flush();
                }
                // System.out.println("完成响应");
            } catch (Exception e) {
                System.out.println("ERROE:" + e.getMessage());
            } finally {
                try {
                    s.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public Push(ArrayBlockingQueue<Message> buffer) {
        m_buffer = buffer;
        m_pool = new ThreadPoolExecutor(8, 8, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(16), new ThreadPoolExecutor.DiscardPolicy());
    }

    @Override
    public void run() {
        try (ServerSocket ss = new ServerSocket(m_port)) {
            System.out.println("出队服务启动");
            while (true) {
                if (m_pool.getTaskCount() - m_pool.getCompletedTaskCount() >= 24) {
                    continue;
                }
                Socket s = ss.accept();
                synchronized (m_pool) {
                    if (m_pool.getTaskCount() - m_pool.getCompletedTaskCount() < 24) {
                        m_pool.execute(new Task(s));
                    } else {
                        s.close();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("ERROE:" + e.toString());
        }
    }
}
