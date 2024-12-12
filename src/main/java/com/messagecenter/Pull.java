package com.messagecenter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Pull implements Runnable {
    public static final int BROADCAST_MODE = 0;
    public static final int CLUSTER_MODE = 1;

    private ArrayBlockingQueue<Message> m_buffer;
    private ThreadPoolExecutor m_pool;
    private Map<String, List<Observer>> m_subscribe;
    private int m_mode = BROADCAST_MODE;
    private Map<String, Integer> m_cycle = new HashMap<>();
    private int m_port = 30000;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    protected static LongAdder m_adder = new LongAdder();

    class Task implements Runnable {
        private Socket s;

        public Task(Socket s) {
            this.s = s;
        }

        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-16LE"))) {
                s.setSoTimeout(2000);
                char[] buff = new char[4096];
                String json = "";

                // System.out.println("开始接收素数");

                do {
                    int len = br.read(buff);
                    if (len == -1)
                        continue;
                    json += String.copyValueOf(buff).substring(0, len);
                    Thread.sleep(10);
                } while (br.ready());

                // System.out.println("已接收素数");

                // System.out.println("接收:" + json);

                Map<String, Object> objectMap = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
                });

                if (objectMap.containsKey("data")) {
                    List<Long> data = MAPPER.readValue(objectMap.get("data").toString(),
                            new TypeReference<List<Long>>() {
                            });
                    for (Long num : data) {
                        Message temp = new Message();
                        temp.time = new Date();
                        temp.tag = "number";
                        temp.value = new Number(num);
                        synchronized (m_adder) {
                            m_adder.add(1);
                            temp.serialNumber = m_adder.longValue();
                            if (m_buffer.offer(temp) == false) {
                                m_buffer.poll();
                                m_buffer.offer(temp);
                            }
                            synchronized (m_subscribe) {
                                try {
                                    if (m_subscribe.get(temp.tag) != null && m_subscribe.get(temp.tag).size() != 0) {
                                        if (m_mode == BROADCAST_MODE) {
                                            for (Observer observer : m_subscribe.get(temp.tag)) {
                                                observer.offer(temp);
                                            }
                                        } else if (m_mode == CLUSTER_MODE) {
                                            if (!m_cycle.containsKey(temp.tag)) {
                                                m_cycle.put(temp.tag, 0);
                                            }
                                            int n = (m_cycle.get(temp.tag) + 1) % m_subscribe.get(temp.tag).size();
                                            int cnt = 0;
                                            for (Observer observer : m_subscribe.get(temp.tag)) {
                                                if (cnt == n) {
                                                    observer.offer(temp);
                                                    break;
                                                }
                                                cnt++;
                                            }
                                            m_cycle.put(temp.tag, n);
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.out.println("ERROE(Pull/Task):" + e.getMessage());
                                }
                            }
                        }
                    }
                    System.out.println("接收数据:" + data.size());
                }
                s.close();
                // System.out.println("完成响应");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("ERROE(Pull):" + e.getMessage());
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

    public Pull(ArrayBlockingQueue<Message> buffer, Map<String, List<Observer>> subscribe) {
        m_buffer = buffer;
        m_pool = new ThreadPoolExecutor(8, 8, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(16), new ThreadPoolExecutor.DiscardPolicy());
        m_subscribe = subscribe;
    }

    public void setMode(int mode) {
        m_mode = mode;
    }

    @Override
    public void run() {
        try (ServerSocket ss = new ServerSocket(m_port)) {
            System.out.println("入队服务启动");
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