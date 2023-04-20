package cis5550.jobs;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class Crawler {
	public static void run(FlameContext context, String[] args) throws Exception {
		if (args.length < 1) {
			context.output("No seed found");
			return;
		}
		List<String> seeds = new ArrayList<String>();
		for (String seed : args) {
			String norm = URLExtractor.normalizeURL("", seed);
			if (norm == null) {
				context.output("seed bad");
				continue;
			}
			seeds.add(seed);
			context.output("Seed added: " + seed);
			System.out.println("Seed added: " + seed);
		}
		FlameRDD urlQueue;
		KVSClient kvsClient = context.getKVS();
		try {
			urlQueue = context.parallelize(seeds);
			kvsClient.persist(Constants.CRAWL);
		} catch (Exception e) {
			e.printStackTrace();
			context.output("KVStore not working.");
			return;
		}
		String kvsMasterAddr = context.getKVS().getMaster();
		context.output("Ready to start crawling");
		System.out.println("Ready to start crawling");
		Thread.sleep(3000);
		while (urlQueue.count() != 0 && kvsClient.count(Constants.CRAWL) < 10000) {
			FlameRDD urlQueueNew = urlQueue.flatMap(urlString -> {
					System.out.println("Crawling " + urlString);
					KVSClient kvs = new KVSClient(kvsMasterAddr);
					String rowKey = Hasher.hash(urlString);
					if (URLExtractor.URLCrawled(rowKey, kvs)) {
						System.out.println("Already attempted");
						return new ArrayList<String>();
					}
					// Filter bad url
					URL url;
					try {
						url = new URL(urlString);
					} catch (Exception e) {
						e.printStackTrace();
						return new ArrayList<String>();
					}
					String[] urlParts = URLParser.parseURL(urlString);
					if (!urlParts[0].equals("https")) {
						return new ArrayList<String>();
					}
					String hostKey = Hasher.hash(urlParts[1]);
					// Register host
					try {
						kvs.put("hosts", hostKey, "url", urlParts[1]);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (hostLimitReached(hostKey, urlParts[1], kvs, 3000)) {
						return new ArrayList<String>();
					}
					Row row = new Row(rowKey);
					row.put("url", urlString);
					if (!RobotsTxtParser.robotPermits(hostKey, urlParts, urlString, kvs, row)) {
						System.out.println("Robot says no");
						return new ArrayList<String>();
					}
					if (!accessTimeLimitPassed(hostKey, kvs)) {
						return Arrays.asList(new String[] {urlString});
					}
					System.out.println("Send HEAD for " + urlString);
					List<String> headRet = sendHead(url, hostKey, rowKey, urlString, kvs, row);
					if (headRet != null) {
						return headRet;
					}
					System.out.println("Send GET for " + urlString);
					return sendGet(url, hostKey, rowKey, urlString, kvs, row, Constants.blacklist);
				});
				urlQueue.delete();
				urlQueue = urlQueueNew;
		}
		context.output("OK");
	}
	
	public static String readBody(HttpURLConnection conn) {
		StringBuilder sb = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			char[] content = new char[1048576]; // 1 MB
			int bytesRead = br.read(content, 0, content.length);
			while (bytesRead != -1) {
				sb.append(content, 0, bytesRead);
				if (sb.length() > 134217728) { // 128 MB
					return null;
				}
				bytesRead = br.read(content, 0, content.length);
			}
			br.close();
		} catch (Exception e) {
			System.out.println("Read failed for URL: " + conn.getURL());
			e.printStackTrace();
			return null;
		}
		return sb.toString();
	}

	public static boolean accessTimeLimitPassed(String hostKey, KVSClient kvs) {
		try {
			byte[] lastAccessTimeBytes = kvs.get("hosts", hostKey, "lastAccessTime");
			if (lastAccessTimeBytes == null) {
				return true;
			}
			long lastAccessTime = Long.parseLong(new String(lastAccessTimeBytes));
			if (System.currentTimeMillis() - lastAccessTime < 1000) {
				return false;
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return true;
		}
	}
	
	// Returns null if we can proceed to GET
	public static List<String> sendHead(URL url, String hostKey, String rowKey, String urlString, KVSClient kvs, Row row) {
		int responseCode;
		try {
			HttpURLConnection connHead = (HttpURLConnection) url.openConnection();
			HttpURLConnection.setFollowRedirects(false);
			connHead.setRequestProperty("User-Agent", "cis5550-crawler");
			connHead.addRequestProperty("accept-language", "en");
			connHead.setRequestMethod("HEAD");
			connHead.setConnectTimeout(30000);
			connHead.setReadTimeout(30000);
			// Update last access time
			kvs.put("hosts", hostKey, "lastAccessTime", String.valueOf(System.currentTimeMillis()));
			connHead.connect();
			// Register response code of HEAD request, also register meta-information of the page.
			responseCode = connHead.getResponseCode();
			row.put("responseCode",String.valueOf(responseCode));
			 // If the response is a redirect, get the destination from body and put it to the back of the queue.
			if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
				kvs.putRow(Constants.CRAWL, row);
				String loc = connHead.getHeaderField("Location");
				String normalized = URLExtractor.normalizeURL(urlString, loc);
				if (normalized == null) {
					return new ArrayList<String>();
				}
				return Arrays.asList(new String[] {normalized});
			}
			// If the response is not 200 or if the content is not text/html, do nothing.
			if (responseCode != 200) {
				kvs.putRow(Constants.CRAWL, row);
				return new ArrayList<String>();
			}
			String contentType = connHead.getContentType();
			if (contentType == null) {
				kvs.putRow(Constants.CRAWL, row);
				return new ArrayList<String>();
			}
			contentType = contentType.trim().toLowerCase();
			row.put("contentType", contentType);
			if (!contentType.startsWith("text/html") || !contentType.contains("utf-8")) {
				kvs.putRow(Constants.CRAWL, row);
				return new ArrayList<String>();
			}
			int contentLength = connHead.getContentLength();
			row.put("length", String.valueOf(contentLength));
			if (contentLength > 1024 * 1024 * 512) { // maximum page size: 512 MB
				kvs.putRow(Constants.CRAWL, row);
				return new ArrayList<String>();
			}
			// See if we can filter out pages that are not in English
			String contentLanguage = connHead.getHeaderField("Content-Language");
			if (contentLanguage != null) {
				if (!contentLanguage.toLowerCase().contains("en")) {
					return new ArrayList<String>();
				}
			}
			return null;
		} catch (Exception e) {
			System.out.println("Exception caused by " + urlString);
			e.printStackTrace();
			return new ArrayList<String>();
		}
	}
	
	public static Set<String> sendGet(URL url, String hostKey, String rowKey, String urlString, KVSClient kvs, Row row, List<String> blacklist) {
		try {
			int responseCode;
			HttpURLConnection connGet = (HttpURLConnection) url.openConnection();
			connGet.setRequestProperty("User-Agent", "cis5550-crawler");
			connGet.setRequestMethod("GET");
			connGet.setConnectTimeout(30000);
			connGet.setReadTimeout(30000);
			// Update access time.
			kvs.put("hosts", hostKey, "lastAccessTime", String.valueOf(System.currentTimeMillis()));
			connGet.connect();
			// Check again if the response code is 200 and if the content type is still text/html, even though it's unlikely that they have changed within such a short amount of time.
			responseCode = connGet.getResponseCode();
			row.put("responseCode", String.valueOf(responseCode));
			if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
				kvs.putRow(Constants.CRAWL, row);
				String loc = connGet.getHeaderField("Location");
				String normalized = URLExtractor.normalizeURL(urlString, loc);
				if (normalized == null) {
					return new HashSet<String>();
				}
				return new HashSet<String>(Arrays.asList(new String[] {normalized}));
			}
			if (responseCode != 200) {
				kvs.putRow(Constants.CRAWL, row);
				return new HashSet<String>();
			}
			String contentType = connGet.getContentType();
			if (contentType == null) {
				kvs.putRow(Constants.CRAWL, row);
				return new HashSet<String>();
			}
			row.put("contentType", contentType);
			contentType = contentType.trim().toLowerCase();
			if (!contentType.startsWith("text/html") || !contentType.contains("utf-8")) {
				kvs.putRow(Constants.CRAWL, row);
				return new HashSet<String>();
			}
			int contentLength = connGet.getContentLength();
			row.put("length", String.valueOf(contentLength));
			if (contentLength > 1024 * 1024 * 512) { // maximum page size: 512 MB
				kvs.putRow(Constants.CRAWL, row);
				return new HashSet<String>();
			}
			String contentLanguage = connGet.getHeaderField("Content-Language");
			if (contentLanguage != null) {
				if (!contentLanguage.toLowerCase().contains("en")) {
					return new HashSet<String>();
				}
			}
			// Finally, read the content of the page and put it to KVS.
			String contentStr = readBody(connGet);
			if (contentStr == null) {
				kvs.putRow(Constants.CRAWL, row);
				return new HashSet<String>();
			}
			if (!pageIsGood(contentStr)) {
				System.out.println("This page is not good.");
				kvs.putRow(Constants.CRAWL, row);
				return new HashSet<String>();
			}
			row.put("page", contentStr);
			kvs.putRow(Constants.CRAWL, row);
			incrementHost(hostKey, kvs);
			System.out.println("Downloaded page: " + urlString);
			// Extract more URLs from this page and put them to the back of the queue.
			return URLExtractor.extractURLs(contentStr, urlString, blacklist, kvs, true);
		} catch (Exception e) {
			e.printStackTrace();
			return new HashSet<String>();
		}
	}
	
	public static void incrementHost(String hostKey, KVSClient kvs) {
		try {
			byte[] numPagesBytes = kvs.get("hosts", hostKey, "numPages");
			if (numPagesBytes == null) {
				kvs.put("hosts", hostKey, "numPages", String.valueOf(1));
				return;
			}
			int numPages = Integer.parseInt(new String(numPagesBytes));
			kvs.put("hosts", hostKey, "numPages", String.valueOf(numPages + 1));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static boolean hostLimitReached(String hostKey, String hostUrl, KVSClient kvs, int limit) {
		try {
			byte[] numPagesBytes = kvs.get("hosts", hostKey, "numPages");
			if (numPagesBytes == null) {
				return false;
			}
			int numPages = Integer.parseInt(new String(numPagesBytes));
			if (numPages > limit) {
				return true;
			}
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return true;
		}
	}
	
	public static boolean pageIsGood(String content) {
		// If we can find an HTML lang tag and the language is not en, ignore the page.
		Pattern pattern = Pattern.compile("<\\s*?html\\s*?.*?\\s*?lang=\"(.*?)\".*?>");
		Matcher matcher = pattern.matcher(content);
		if (matcher.find()) {
			String lang = matcher.group(1);
			if (!lang.contains("en")) {
				return false;
			}
		}
		//Other filters?
		return true;
	}
	
}
