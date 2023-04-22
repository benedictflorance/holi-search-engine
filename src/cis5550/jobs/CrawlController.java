package cis5550.jobs;

import java.io.IOException;
import java.util.Iterator;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class CrawlController {
	public static void main(String[] args) throws IOException {
		// String key = Hasher.hash("https://www.rottentomatoes.com:443/");
		// printRow("crawl", key);
		System.out.println(seeNumPages("https://www.rottentomatoes.com:443/"));
	}
	
	public static String seeNumPages(String url) throws IOException {
		KVSClient kvs = new KVSClient("localhost:8000");
		String[] urlParts = URLParser.parseURL(url);
		String hostKey = Hasher.hash(urlParts[1]);
		Row r = kvs.getRow("hosts", hostKey);
		return r.get("numPages") + "/" + r.get("quota");
	}
	
	public static void setQuota(String quota, String url) throws IOException {
		KVSClient kvs = new KVSClient("localhost:8000");
		String[] urlParts = URLParser.parseURL(url);
		String hostKey = Hasher.hash(urlParts[1]);
		kvs.put("hosts", hostKey, "quota", quota);
		Row r = kvs.getRow("hosts", hostKey);
		System.out.println(r.get("numPages") + "/" + r.get("quota"));
	}
	
	public static void printRow(String table, String rowKey) throws IOException {
		KVSClient kvs = new KVSClient("localhost:8000");
		Row r = kvs.getRow(table, rowKey);
		System.out.println(r.toString());
	}
}
