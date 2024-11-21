package com.messagecenter;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Subscribe implements Runnable {
    private List<Observer> m_subscribe;
    private int m_port = 50000;
    private ThreadPoolExecutor m_pool = new ThreadPoolExecutor(8, 8, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(16), new ThreadPoolExecutor.DiscardPolicy());

    public Subscribe(List<Observer> subscribe) {
        m_subscribe = subscribe;
    }

    @Override
    public void run() {
        try (ServerSocket ss = new ServerSocket(m_port)) {
            System.out.println("订阅服务启动");
            while (true) {
                Observer observer = new Observer(ss.accept(), new ArrayBlockingQueue<>(100000));
                m_subscribe.add(observer);
                m_pool.execute(observer);
            }
        } catch (Exception e) {
            System.out.println("ERROE:" + e.toString());
        }
    }
}