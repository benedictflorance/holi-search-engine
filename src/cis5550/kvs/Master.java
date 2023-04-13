package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.util.Map.Entry;

public class Master extends cis5550.generic.Master{
	
	public static int port;
	
	public static void main(String args[]) throws Exception {
		port = Integer.parseInt(args[0]);
		
		port(port);
		
		registerRoutes();
		
		get("/", (req,res) -> { 
			res.status(200, "OK");
			res.type("text/html");
			return workerTable(); 
		});
		
		Thread daemonThread = new Thread(){
		    public void run(){
		    	while(true) {
					try {
						 for (Entry<String, cis5550.generic.Worker> entry : activeWorkers.entrySet()) { 
							 cis5550.generic.Worker worker = entry.getValue();
							 if(System.currentTimeMillis() - worker.getLastAccessedTime()>15000) {
								System.out.println("Remove worker");
								// activeWorkers.remove(entry.getKey(), entry.getValue());
							 }
						 }
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		    }
		};
		daemonThread.start();
	}
}

