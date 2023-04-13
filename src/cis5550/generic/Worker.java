package cis5550.generic;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class Worker {
	private String id;
	private String ip;
	private String port;
	private long lastAccessedTime;
	
	public static void startPingThread(String master, String id, int port) throws MalformedURLException {
		Thread pt = new Thread() {
			URL url = new URL("http://" + master + "/ping?id=" + id + "&port=" + String.valueOf(port));
			@Override
			public void run() {
				while (true) {
					try {
						HttpURLConnection conn = (HttpURLConnection) url.openConnection();
						conn.getResponseCode();
						Thread.sleep(5000);
					} catch (Exception e) {
						continue;
					}
				}
			}
			
		};
		pt.start();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public long getLastAccessedTime() {
		return lastAccessedTime;
	}

	public void setLastAccessedTime(long lastAccessedTime) {
		this.lastAccessedTime = lastAccessedTime;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
}
