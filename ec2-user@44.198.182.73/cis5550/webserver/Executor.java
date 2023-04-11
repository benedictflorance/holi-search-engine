package cis5550.webserver;

import java.util.concurrent.BlockingQueue;

// The main class of your server should be cis5550.webserver.Server.
class Executor implements Runnable {
    BlockingQueue<Runnable> blockingQueue;
    public Executor(BlockingQueue<Runnable> queue)
    {
        this.blockingQueue = queue;
    }
    @Override
    public void run(){
        while(true)
        {
            Runnable task = null;
            try {
                task = blockingQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            task.run();
        }
    }
}