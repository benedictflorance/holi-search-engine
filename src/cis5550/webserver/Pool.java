package cis5550.webserver;

import cis5550.webserver.Executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class Pool{
    BlockingQueue<Runnable> blockingQueue;
    public Pool(int num_workers){
        blockingQueue = new LinkedBlockingQueue<Runnable>();
        Executor task = null;
        for(int i = 0; i < num_workers; i++)
        {
            task = new Executor(blockingQueue);
            Thread t = new Thread(task);
            t.start();
        }
    }
    public void submit(Runnable task) throws InterruptedException {
        blockingQueue.put(task);
    }
}