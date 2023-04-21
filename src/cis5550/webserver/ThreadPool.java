package cis5550.webserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ThreadPool {
	
	private BlockingQueue<Task> tasks;
	private List<Thread> workers;
	int numBusy;
	
	public ThreadPool() {
		tasks = new LinkedBlockingQueue<Task>();
		workers = new ArrayList<Thread>();
		numBusy = 0;
	}
	
	public void spawnWorkers(int num_workers) {
		for (int i = 0; i < num_workers; i++) {
			Thread t = new SisyphusLoop(i);
			t.start();
			workers.add(t);
		}
		try {
			// Wait for all threads to be ready before returning.
			Thread.sleep(2000);
		} catch (InterruptedException e) {

		}
	}
	
	public void killWorkers() {
		for (Thread st : workers) {
			SisyphusLoop st_ = (SisyphusLoop) st;
			st_.kill();
		}
	}
	
	public void dispatch(Task t) {
		try {
			tasks.put(t);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	// Inherit from this class if you want to use this thread pool
	public interface Task {
		public void run();
	}
	
	private synchronized void incrementBusy(int id) {
		numBusy++;
		if (workers.size() - numBusy < 5) {
			System.out.println("Thread pool capacity: " + numBusy + "/" + workers.size());
		}
	}
	private synchronized void decrementBusy(int id) {
		numBusy--;
	}
	
	private class SisyphusLoop extends Thread {
		private boolean running = true;
		public int id;
		public SisyphusLoop(int id) {this.id = id;}
		public void kill() {running = false;}
		public void run() {
			try {
				while (running) {
					Task task = tasks.take();
					incrementBusy(id);
					task.run();
					decrementBusy(id);
				}
			} catch (Exception e) {

			}
		}
	}
}
