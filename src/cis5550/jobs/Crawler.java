package cis5550.jobs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
		context.output("OK");
		String seed = normalizeURL("", args[0]);
		FlameRDD urlQueue = context.parallelize(Arrays.asList(seed));
		context.getKVS().persist("crawl");
		String kvsMasterAddr = context.getKVS().getMaster();
		while (urlQueue.count() != 0) {
			urlQueue = urlQueue.flatMap(urlString -> {
				// If the url exists in the queue, return
				KVSClient kvs = new KVSClient(kvsMasterAddr);
				String rowKey = Hasher.hash(urlString);
				try {
					if (kvs.existsRow("crawl", rowKey)) {
						return new ArrayList<String>();
					}
				} catch (Exception e) {

				}
				String[] host = URLParser.parseURL(urlString);
				if (host[0] == null || host[1] == null) {
					return new ArrayList<String>();
				}
				String hostKey = Hasher.hash(host[1]);
				try {
					kvs.put("hosts", hostKey, "url", host[1]);
				} catch (Exception e) {

				}
				int responseCode = 0;
				// Create url
				URL url = new URL(urlString);
				// Check robot.txt
				try {
					byte[] robotBytes = kvs.get("hosts", hostKey, "robots.txt");
					if (robotBytes != null ) { // robots.txt has been requested
						String robot = new String(robotBytes);
						if (!robot.equals("FALSE")) { // there is a robots.txt
							if (!parseRobotTxt(robot, host[3])) {
								return new ArrayList<String>();
							}
						} 
						// if robot.txt == "FALSE", the host does not have a robots.txt, we can crawl freely.
					} else { //robot.txt has not been requested
						URL urlRobo = new URL(host[0] + "://" + host[1] + "/robots.txt");
						HttpURLConnection connRobo = (HttpURLConnection) urlRobo.openConnection();
						connRobo.setRequestProperty("User-Agent", "cis5550-crawler");
						connRobo.setRequestMethod("GET");
						connRobo.connect();
						responseCode = connRobo.getResponseCode();
						if (responseCode != 200) { // The host has no robots.txt
							kvs.put("hosts", hostKey, "robots.txt", "FALSE");
						} else {
							String robot = readBody(connRobo);
							if (robot != null) {
								kvs.put("hosts", hostKey, "robots.txt", robot.getBytes());
								if (!parseRobotTxt(robot, host[3])) {
									return new ArrayList<String>();
								}
							} else {
								kvs.put("hosts", hostKey, "robots.txt", "FALSE");
							}
						}
					}
				} catch (Exception e) {
					return new ArrayList<String>();
				}
				// Check time
				try {
					byte[] lastAccessTimeBytes = kvs.get("hosts", hostKey, "lastAccessTime");
					if (lastAccessTimeBytes != null) {
						long lastAccessTime = Long.parseLong(new String(lastAccessTimeBytes));
						if (System.currentTimeMillis() - lastAccessTime < 1000) {
							return Arrays.asList(new String[] {urlString});
						}
					}
				} catch (Exception e) {
					return Arrays.asList(new String[] {urlString});
				}
				
				HttpURLConnection connHead = (HttpURLConnection) url.openConnection();
				HttpURLConnection.setFollowRedirects(false);
				connHead.setRequestProperty("User-Agent", "cis5550-crawler");
				connHead.setRequestMethod("HEAD");
				try {
					kvs.put("hosts", hostKey, "lastAccessTime", String.valueOf(System.currentTimeMillis()));
					connHead.connect();
				} catch (Exception e) {
					return new ArrayList<String>();
				}
				responseCode = connHead.getResponseCode();
				Row row = new Row(rowKey);
				try {
					row.put("responseCode",String.valueOf(responseCode));
					row.put("url", urlString);
					String contentType = connHead.getContentType();
					if (contentType != null) {
						row.put("contentType", contentType);
					}
					int contentLength = connHead.getContentLength();
				    row.put("length", String.valueOf(contentLength));
					
				} catch (Exception e) {
					
				}
				if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
					kvs.putRow("crawl", row);
					String loc = connHead.getHeaderField("Location");
					String normalized = normalizeURL(urlString, loc);
					if (normalized == null) {
						return new ArrayList<String>();
					}
					return Arrays.asList(new String[] {normalized});
				}
				if (responseCode != 200 || !connHead.getContentType().toLowerCase().equals("text/html")) {
					kvs.putRow("crawl", row);
					return new ArrayList<String>();
				}
				HttpURLConnection connGet = (HttpURLConnection) url.openConnection();
				connGet.setRequestProperty("User-Agent", "cis5550-crawler");
				connGet.setRequestMethod("GET");
				try {
					kvs.put("hosts", hostKey, "lastAccessTime", String.valueOf(System.currentTimeMillis()));
					connGet.connect();
				} catch (Exception e) {
					return new ArrayList<String>();
				}
				responseCode = connGet.getResponseCode();
				row.put("responseCode",String.valueOf(responseCode));
				if (responseCode != 200 || !connGet.getContentType().toLowerCase().equals("text/html")) {
					return new ArrayList<String>();
				}
				String contentStr = readBody(connGet);
				row.put("page", contentStr);
				kvs.putRow("crawl", row);
				List<String> newURLs = extractURLs(contentStr, urlString);
				return newURLs;
			});
			Thread.sleep(1200);
		}
	}
	
	public static String readBody(HttpURLConnection conn) {
		String body = new String();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			char[] content = new char[4096];
			int bytesRead = br.read(content, 0, 4096);
			while (bytesRead != -1) {
				body += new String(content, 0, bytesRead);
				bytesRead = br.read(content, 0, 4096);
			}
			br.close();
		} catch (Exception e) {
			return null;
		}
		return body;
		
	}
	
	public static List<String> extractURLs(String content, String baseURL) {
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("<.*href=\"(.*?)\".*?>", java.util.regex.Pattern.CASE_INSENSITIVE);
		java.util.regex.Matcher matcher = pattern.matcher(content);
		List<String> newURLs = new ArrayList<String>();
	    while(matcher.find()) {
	    	String newURL = matcher.group(1);
	    	String newURLNorm = normalizeURL(baseURL, newURL);
	    	if (newURLNorm != null) {
	    		newURLs.add(newURLNorm);
	    	}
	    }
	    return newURLs;
	}
	
	public static String normalizeURL(String base, String url) {
		String[] baseP = URLParser.parseURL(base);
		String[] urlP = URLParser.parseURL(url);
		if (baseP[2] == null) {
			baseP[2] = "80";
		}
		if (urlP[0] == null && urlP[1] == null && urlP[2] == null && urlP[3] == null) {
			return null;
		}
		// internal url
		if (urlP[0] == null && urlP[1] == null && urlP[2] == null) {
			if (urlP[3].endsWith(".jpg") || urlP[3].endsWith(".jpeg") || urlP[3].endsWith(".gif") || urlP[3].endsWith(".png")|| urlP[3].endsWith(".txt")) {
				return null;
			}
			if (urlP[3].charAt(0) == '#') {
				return baseP[0] + "://" + baseP[1] + ":" + baseP[2] + baseP[3];
			}
			int pound = urlP[3].indexOf('#');
			int end = pound < 0 ? urlP[3].length() : pound;
			// absolute
			if (urlP[3].charAt(0) == '/') {
				return baseP[0] + "://" + baseP[1] + ":" + baseP[2] + urlP[3].substring(0, end);
			}
			// relative 
			String[] cutRes = cut(baseP[3], urlP[3].substring(0, end));
			return baseP[0] + "://" + baseP[1] + ":" + baseP[2] + cutRes[0] + "/" + cutRes[1];
		}
		// external url
		if (!urlP[0].equals("http") && !urlP[0].equals("https")) {
			return null;
		}
		if (urlP[1] == null) {
			return null;
		}
		if (urlP[3] != null) {
			if (urlP[3].endsWith(".jpg") || urlP[3].endsWith(".jpeg") || urlP[3].endsWith(".gif") || urlP[3].endsWith(".png")|| urlP[3].endsWith(".txt")) {
				return null;
			}
		}
		if (urlP[2] == null) {
			urlP[2] = "80";
		}
		return urlP[0] + "://" + urlP[1] + ":" + urlP[2] + urlP[3];
	}

	public static String[] cut(String base, String url) {
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
		System.out.println(url);
		int i = url.length() - 1;
		while (url.charAt(i) != '/') {
			i--;
		}
		return url.substring(0, i);
	}

	public static boolean parseRobotTxt(String robotTxt, String url) {
		int agent = robotTxt.indexOf("User-agent: cis5550-crawler");
		if (agent >= 0) {
			java.io.BufferedReader reader = new BufferedReader(new java.io.StringReader(robotTxt.substring(agent + 28)));
			try {
				String line = reader.readLine();
				while (line != null) {
					line = line.trim();
					if (line.length() == 0) {
						line = reader.readLine();
						continue;
					}
					if (line.charAt(0) == '#') {
						line = reader.readLine();
						continue;
					}
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
						if (url.startsWith(disallowStr)) {
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
						if (url.startsWith(allowStr)) {
							return true;
						}
						line = reader.readLine();
						continue;
					}
					line = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				
			}
			return true;
		} 
		// no user-agent specific rule
		int wild = robotTxt.indexOf("User-agent: *");
		// No rule applies
		if (wild < 0) {
			return true;
		}
		// wild card
		java.io.BufferedReader reader = new BufferedReader(new java.io.StringReader(robotTxt.substring(wild + 13)));
		try {
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.length() == 0) {
					line = reader.readLine();
					continue;
				}
				if (line.charAt(0) == '#') {
					line = reader.readLine();
					continue;
				}
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
					if (url.startsWith(disallowStr)) {
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
					if (url.startsWith(allowStr)) {
						return true;
					}
					line = reader.readLine();
					continue;
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (Exception e) {
			
		}
		return true;
	}
}
