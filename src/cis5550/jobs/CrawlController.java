package cis5550.jobs;

import java.io.IOException;

import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;

public class CrawlController {
	public static void main(String[] args) throws IOException {
		KVSClient kvs = new KVSClient("localhost:8000");
		String rKey = Hasher.hash("https://www.condenast.com:443/");
		kvs.put("hosts", rKey, "robots.txt", "IGNORE");
	}
}
