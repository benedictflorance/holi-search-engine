package cis5550.jobs;

import java.io.IOException;
import java.util.Iterator;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class CrawlController {
	public static void main(String[] args) throws IOException {
		KVSClient kvs = new KVSClient("localhost:8000");
		Iterator<Row> hosts = kvs.scan("hosts");
		while (hosts.hasNext()) {
			Row r = hosts.next();
			r.put("quota", "10000");
			kvs.putRow("hosts", r);
		}
	}
}
