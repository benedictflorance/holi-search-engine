package cis5550.jobs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
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
		String seed = normalizeURL("", args[0]);
		FlameRDD urlQueue;
		KVSClient kvsClient = context.getKVS();
		try {
			urlQueue = context.parallelize(Arrays.asList(seed));
			kvsClient.persist("crawl");
		} catch (Exception e) {
			e.printStackTrace();
			context.output("KVStore not working.");
			return;
		}
		String kvsMasterAddr = context.getKVS().getMaster();
		List<String> blacklist = new ArrayList<String>(Arrays.asList(buildBadURLsList()));
		// Start crawling
		while (urlQueue.count() != 0 && kvsClient.count("crawl") < 20000) {
				urlQueue = urlQueue.flatMap(urlString -> {
					System.out.println("Crawling " + urlString);
					KVSClient kvs = new KVSClient(kvsMasterAddr);
					String rowKey = Hasher.hash(urlString);
					if (URLCrawled(rowKey, kvs)) {
						System.out.println("Already crawled");
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
					if (!robotPermits(hostKey, urlParts, urlString, kvs)) {
						System.out.println("Robot says no");
						return new ArrayList<String>();
					}
					if (!accessTimeLimitPassed(hostKey, kvs)) {
						System.out.println("last access too recent");
						return Arrays.asList(new String[] {urlString});
					}
					Row row = new Row(rowKey);
					System.out.println("Send HEAD for " + urlString);
					List<String> headRet = sendHead(url, hostKey, rowKey, urlString, kvs, row);
					if (headRet != null) {
						return headRet;
					}
					System.out.println("Send GET for " + urlString);
					return sendGet(url, hostKey, rowKey, urlString, kvs, row,blacklist);
				});
		}
		context.output("OK");
	}
	
	public static String readBody(HttpURLConnection conn) {
		String body = new String();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			char[] content = new char[1048576];
			int bytesRead = br.read(content, 0, 1048576); // 1 MB
			while (bytesRead != -1) {
				body += new String(content, 0, bytesRead);
				bytesRead = br.read(content, 0, 1048576); // 1 MB
			}
			br.close();
		} catch (Exception e) {
			System.out.println("Read failed for URL: " + conn.getURL());
			e.printStackTrace();
			return null;
		}
		return body;
	}
	
	public static boolean URLCrawled(String urlHash, KVSClient kvs) {
		try {
			if (kvs.existsRow("crawl", urlHash)) {
				return true;
			}
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return true;
		}
	}
	
	public static Set<String> extractURLs(String content, String baseURL, List<String> blacklist) {
		System.out.println("Downloaded page: " + baseURL);
	    Pattern pattern = Pattern.compile("<\\s*?a\\s+[^>]*href=\\s*?\"(.*?)\".*?>", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(content);
		Set<String> newURLs = new HashSet<String>();
	    while(matcher.find()) {
	    	String newURL = matcher.group(1);
	    	String newURLNorm = normalizeURL(baseURL, newURL);
	    	if (newURLNorm == null) {
	    		continue; 
	    	}
	    	try {
	    		new URL(newURLNorm);
	    	} catch (Exception e) {
	    		continue;
	    	}
	    	if (isBlacklisted(newURLNorm, blacklist)) {
	    		continue;
	    	}
	    	newURLs.add(newURLNorm);
	    }
	    return newURLs;
	}
	
	public static String normalizeURL(String base, String url) {
		// Parse results:
		// 0: http or https
		// 1: host name
		// 2: port
		// 3: uri
		String[] baseP = URLParser.parseURL(base);
		String[] urlP = URLParser.parseURL(url);
		if (baseP[2] == null) {
			if (baseP[0] != null) {
				if (baseP[0].equals("http")) {
					baseP[2] = "80";
				} else if (baseP[0].equals("https")) {
					baseP[2] = "443";
				}
			}
		}
		if (urlP[0] == null && urlP[1] == null && urlP[2] == null && urlP[3] == null) {
			return null;
		}
		// internal url
		if (urlP[0] == null && urlP[1] == null && urlP[2] == null) {
			if (baseP[0] == null || baseP[1] == null) {
				// Can't make it work if URL is internal and base URL is malformed.
				return null;
			}
			if (urlP[3].length() == 0) {
				return null;
			}
			String uriClean = removeParams(urlP[3]);
			if (endsWithUnwanted(uriClean)) {
				return null;
			}
			if (uriClean.length() == 0) {
				return null;
			}
			// absolute path
			if (uriClean.charAt(0) == '/') {
				return baseP[0] + "://" + baseP[1] + ":" + baseP[2] + uriClean;
			}
			// relative path
			String[] cutRes = cutRelativePath(baseP[3], uriClean);
			return baseP[0] + "://" + baseP[1] + ":" + baseP[2] + cutRes[0] + "/" + cutRes[1];
		}
		// external url
		if (!urlP[0].equals("https")) {
			return null;
		}
		if (urlP[1] == null) {
			return null;
		}
		if (urlP[2] == null) {
			if (urlP[0].equals("http")) {
				urlP[2] = "80";
			} else if (urlP[0].equals("https")) {
				urlP[2] = "443";
			}
		}
		if (urlP[3] == null) {
			return urlP[0] + "://" + urlP[1] + ":" + urlP[2];
		}
		String uriClean = removeParams(urlP[3]);
		if (endsWithUnwanted(uriClean)) {
			return null;
		}
		return urlP[0] + "://" + urlP[1] + ":" + urlP[2] + uriClean;
	}
	public static boolean endsWithUnwanted(String uri) {
		// String[] unwantedList = {".jpg", ".jpeg", ".gif", ".png", ".txt", ".pdf", ".aspx", ".asp", ".cfml", ".cfm", ".js", ".css", ".c", ".cpp", ".cc", ".java", ".sh", ".obj", ".o", ".h", ".hpp", ".json", ".env", ".class", ".php", ".php3", ".py"};
		// java.util.Set<String> unwanted = new java.util.HashSet<String>();
		if (uri.length() == 0) {
			return false;
		}
		if (uri.contains("@")) {
			return true;
		}
		if (uri.contains("javascript:")) {
			return true;
		}
		if (!uri.contains(".")) {
			return false;
		}
		// If the url contains a . and ends not with .html, we don't want the uri
		if (!uri.endsWith(".html")) {
			return true;
		}
		return false;
	}
	public static String[] cutRelativePath(String base, String url) {
		String[] res = new String[2];
		base = cutToLastSlash(base);
		int dotdot = url.indexOf("../");
		while (dotdot >= 0) {
			base = cutToLastSlash(base);
			url = url.substring(dotdot + 3);
			dotdot = url.indexOf("../");
		}
		res[0] = base;
		res[1] = url;
		return res;
	}
	
	public static String cutToLastSlash(String url) {
		if (url.length() < 1) {
			return url;
		}
		int i = url.length() - 1;
		while (url.charAt(i) != '/') {
			i--;
		}
		return url.substring(0, i);
	}
	
	public static String removeParams(String uri) {
		int i = 0;
		while (i < uri.length()) {
			if (uri.charAt(i) == '#' || uri.charAt(i) == '?' || uri.charAt(i) == '=') {
				break;
			}
			i++;
		}
		return uri.substring(0, i);
	}
	
	public static boolean parseRules(BufferedReader reader, String url) {
		try {
			String line = reader.readLine();
			boolean seenRule = false;
			while (line != null) {
				line = line.trim();
				if (line.length() == 0) {
					line = reader.readLine();
					continue;
				}
				if (seenRule && line.startsWith("User-agent")) {
					// Rule ends.
					break;
				}
				if (line.charAt(0) == '#') {
					line = reader.readLine();
					continue;
				}
				seenRule = true;
				int disallow = line.indexOf("Disallow:");
				if (disallow >= 0) { // There exists a disallow rule
					String disallowStr = line.substring(disallow + 9).trim();
					if (disallowStr.length() == 0) {
						line = reader.readLine();
						continue;
					}
					if (disallowStr.charAt(0) != '/') {
						line = reader.readLine();
						continue;
					}
					disallowStr = disallowStr.trim().replace("?", "[\\\\?]").replace("*", ".*?").replace("!", "[\\\\!]").replace("/", "\\/") + ".*?";
					Pattern pattern = Pattern.compile(disallowStr);
					Matcher matcher = pattern.matcher(url);
					if (matcher.matches()) {
						return false;
					}
					line = reader.readLine();
					continue;
				}
				int allow = line.indexOf("Allow:");
				if (allow >= 0) {
					String allowStr = line.substring(allow + 6).trim();
					if (allowStr.length() == 0) {
						line = reader.readLine();
						continue;
					}
					if (allowStr.charAt(0) != '/') {
						line = reader.readLine();
						continue;
					}
					allowStr = allowStr.trim().replace("?", "[\\\\?]").replace("*", ".*?").replace("!", "[\\\\!]").replace("/", "\\/") + ".*?";
					Pattern pattern = Pattern.compile(allowStr);
					Matcher matcher = pattern.matcher(url);
					if (matcher.matches()) {
						return true;
					}
					line = reader.readLine();
					continue;
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
			return true;
		}
		return true;
	}

	public static boolean parseRobotsTxt(String robotTxt, String url) {
		int agent = robotTxt.indexOf("User-agent: cis5550-crawler");
		if (agent >= 0) {
			BufferedReader reader = new BufferedReader(new StringReader(robotTxt.substring(agent + 28)));
			return parseRules(reader, url);
		} 
		// no user-agent specific rule
		int wild = robotTxt.indexOf("User-agent: *");
		// No rule applies
		if (wild < 0) {
			return true;
		}
		// wild card
		BufferedReader reader = new BufferedReader(new StringReader(robotTxt.substring(wild + 13)));
		return parseRules(reader, url);
	}
	
	public static boolean robotPermits(String hostKey, String[] urlParts, String wholeURL, KVSClient kvs) {
		try {
			int responseCode;
			// Check if robot.txt has been requested for this host.
			byte[] robotBytes = kvs.get("hosts", hostKey, "robots.txt");
			if (robotBytes != null ) {
				// robots.txt has been requested
				String robot = new String(robotBytes);
				if (robot.equals("FALSE")) {
					// if robot.txt == "FALSE", we have already verified that this host does not have a robots.txt, so we can crawl freely.
					return true;
				}
				// This host has a robots.txt
				if (!parseRobotsTxt(robot, urlParts[3])) {
					// robots.txt forbids crawling of this page.
					return false;
				}
				return true;
			}
			//robot.txt has not been requested from this host. Request it.
			URL urlRobo = new URL(urlParts[0] + "://" + urlParts[1] + "/robots.txt");
			HttpURLConnection connRobo = (HttpURLConnection) urlRobo.openConnection();
			connRobo.setRequestProperty("User-Agent", "cis5550-crawler");
			connRobo.setRequestMethod("GET");
			connRobo.setConnectTimeout(30000);
			connRobo.setReadTimeout(30000);
			connRobo.connect();
			responseCode = connRobo.getResponseCode();
			if (responseCode != 200) {
				// The host has no robots.txt. Mark as FALSE so that future queries know.
				kvs.put("hosts", hostKey, "robots.txt", "FALSE");
				return true;
			}
			int roboLength = connRobo.getContentLength();
			if (roboLength > 1024 * 1024 * 512) {
				kvs.put("hosts", hostKey, "robots.txt", "FALSE");
				return true;
			}
			// This host has a robots.txt
			String robot = readBody(connRobo);
			if (robot == null) {
				// The host has no robots.txt. Mark as FALSE so that future queries know.
				kvs.put("hosts", hostKey, "robots.txt", "FALSE");
				return true;
			}
			// Save robots.txt to KVS
			kvs.put("hosts", hostKey, "robots.txt", robot.getBytes());
			if (!parseRobotsTxt(robot, urlParts[3])) {
				// robots.txt forbids crawling of this page.
				return false;
			}
			return true;
		}  catch (Exception e) {
			try {
				kvs.put("hosts", hostKey, "robots.txt", "FALSE");
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			System.out.println("Exception caused by " + wholeURL);
			e.printStackTrace();
			return false;
		}
	}
	public static boolean accessTimeLimitPassed(String hostKey, KVSClient kvs) {
		try {
			byte[] lastAccessTimeBytes = kvs.get("hosts", hostKey, "lastAccessTime");
			if (lastAccessTimeBytes == null) {
				return true;
			}
			long lastAccessTime = Long.parseLong(new String(lastAccessTimeBytes));
			if (System.currentTimeMillis() - lastAccessTime < 1000) {
				// If we have accessed this host within 1 second, put this page to the back of the queue to crawl later.
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
			row.put("url", urlString);
			 // If the response is a redirect, get the destination from body and put it to the back of the queue.
			if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
				kvs.putRow("crawl", row);
				String loc = connHead.getHeaderField("Location");
				String normalized = normalizeURL(urlString, loc);
				if (normalized == null) {
					return new ArrayList<String>();
				}
				return Arrays.asList(new String[] {normalized});
			}
			// If the response is not 200 or if the content is not text/html, do nothing.
			if (responseCode != 200) {
				kvs.putRow("crawl", row);
				return new ArrayList<String>();
			}
			String contentType = connHead.getContentType();
			if (contentType == null) {
				kvs.putRow("crawl", row);
				return new ArrayList<String>();
			}
			contentType = contentType.trim().toLowerCase();
			row.put("contentType", contentType);
			if (!contentType.startsWith("text/html") || !contentType.contains("utf-8")) {
				kvs.putRow("crawl", row);
				return new ArrayList<String>();
			}
			int contentLength = connHead.getContentLength();
			row.put("length", String.valueOf(contentLength));
			if (contentLength > 1024 * 1024 * 512) { // maximum page size: 512 MB
				kvs.putRow("crawl", row);
				return new ArrayList<String>();
			}
			// See if we can filter out pages that are not in English
			String contentLanguage = connHead.getHeaderField("Content-Language");
			if (contentLanguage == null) {
				return new ArrayList<String>();
			}
			if (!contentLanguage.toLowerCase().contains("en")) {
				return new ArrayList<String>();
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
				kvs.putRow("crawl", row);
				String loc = connGet.getHeaderField("Location");
				String normalized = normalizeURL(urlString, loc);
				if (normalized == null) {
					return new HashSet<String>();
				}
				return new HashSet<String>(Arrays.asList(new String[] {normalized}));
			}
			if (responseCode != 200) {
				kvs.putRow("crawl", row);
				System.out.println("response code");
				return new HashSet<String>();
			}
			String contentType = connGet.getContentType();
			if (contentType == null) {
				System.out.println("content type 1");
				kvs.putRow("crawl", row);
				return new HashSet<String>();
			}
			row.put("contentType", contentType);
			contentType = contentType.trim().toLowerCase();
			if (!contentType.startsWith("text/html") || !contentType.contains("utf-8")) {
				System.out.println("content type 2");
				kvs.putRow("crawl", row);
				return new HashSet<String>();
			}
			int contentLength = connGet.getContentLength();
			row.put("length", String.valueOf(contentLength));
			if (contentLength > 1024 * 1024 * 512) { // maximum page size: 512 MB
				System.out.println("content size ");
				kvs.putRow("crawl", row);
				return new HashSet<String>();
			}
			String contentLanguage = connGet.getHeaderField("Content-Language");
			if (contentLanguage == null) {
				System.out.println("content lang 1");
				return new HashSet<String>();
			}
			if (!contentLanguage.toLowerCase().contains("en")) {
				System.out.println("content lang 2");
				return new HashSet<String>();
			}
			// Finally, read the content of the page and put it to KVS.
			String contentStr = readBody(connGet);
			if (contentStr == null) {
				System.out.println("body 1");
				kvs.putRow("crawl", row);
				return new HashSet<String>();
			}
			row.put("page", contentStr);
			kvs.putRow("crawl", row);
			// Extract more URLs from this page and put them to the back of the queue.
			return extractURLs(contentStr, urlString, blacklist);
		} catch (Exception e) {
			e.printStackTrace();
			return new HashSet<String>();
		}
	}
	
	public static boolean isBlacklisted(String url, List<String> blacklist) {
        for (String b : blacklist) {
        	Pattern pattern = Pattern.compile(b);
        	Matcher matcher = pattern.matcher(url);
            if (matcher.find()) {
                return true;
            }
        }
        return false;
    }
	
	public static String[] buildBadURLsList() {
		String proto = "http.*:\\/\\/";
		return new String[] {proto + ".*\\/cgi-bin\\/.*",
							 proto + ".*\\/javascript\\/.*",
							 proto + ".*\\.appfinders\\.com*",
							 proto + "[www.]*youtube\\.com*",
							 proto + "[www.]*flickr\\.com*"};
	}
	
}
