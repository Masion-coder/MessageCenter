package com.messagecenter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Subscribe implements Runnable {
    private ArrayBlockingQueue<Message> m_buffer;
    private Map<String, List<Observer>> m_subscribe;
    private int m_port = 20000;
    private ThreadPoolExecutor m_pool = new ThreadPoolExecutor(8, 8, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(16), new ThreadPoolExecutor.DiscardPolicy());

    Timer update = new Timer();
        

    class Update extends TimerTask {
            @Override
            public void run() {
                m_subscribe.forEach((key, value) -> {
                    List<Observer> newValue = new LinkedList<>();
                    for (Observer observer : value) {
                        try {
                            BufferedWriter bw = new BufferedWriter(
                                    new OutputStreamWriter(observer.getSocket().getOutputStream(), "UTF-16LE"));
                            bw.write(' ');
                            bw.flush();
                            newValue.add(observer);
                        } catch (IOException e) {
                            System.err.println(observer.getName() + "已取消订阅");
                        }
                        m_subscribe.put(key, newValue);
                    }
                });
            }
        }

    public Subscribe(ArrayBlockingQueue<Message> buffer, Map<String, List<Observer>> subscribe) {
        m_buffer = buffer;
        m_subscribe = subscribe;
        update.schedule(new Update(), 10, 10);
    }

    @Override
    public void run() {
        try (ServerSocket ss = new ServerSocket(m_port)) {
            System.out.println("订阅服务启动");
            Observer.m_que = m_buffer;
            Observer.m_subscribe = m_subscribe;
            while (true) {
                Observer observer = new Observer(ss.accept(), new ArrayBlockingQueue<>(1000000));
                m_pool.execute(observer);
            }
        } catch (Exception e) {
            System.out.println("ERROE:" + e.toString());
        }
    }
}