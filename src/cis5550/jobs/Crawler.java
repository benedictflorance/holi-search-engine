package cis5550.jobs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
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
		KVSClient kvsClient = context.getKVS();
		kvsClient.persist("crawl");
		String kvsMasterAddr = context.getKVS().getMaster();
		String proto = "http.*:\\/\\/";
		String[] badURLs = {proto + ".*/cgi-bin/.*",
							proto + ".*\\.appfinders\\.com*",
							proto + "[www.]*youtube\\.com*",
							proto + "[www.]*flickr\\.com*"};
		List<String> blacklist = new ArrayList<String>(Arrays.asList(badURLs));
		// Start crawling
		while (urlQueue.count() != 0 && kvsClient.count("crawl") < 10000) {
			urlQueue = urlQueue.flatMap(urlString -> {
				KVSClient kvs = new KVSClient(kvsMasterAddr);
				String rowKey = Hasher.hash(urlString);
				try {
					if (kvs.existsRow("crawl", rowKey)) {
						// If we have crawled this URL, do nothing
						return new ArrayList<String>();
					}
				} catch (Exception e) {
					e.printStackTrace();
					return new ArrayList<String>(Arrays.asList(urlString));
				}
				// Filter bad url
				String[] host = URLParser.parseURL(urlString);
				if (host[0] == null || host[1] == null) {
					return new ArrayList<String>();
				}
				// Register host
				String hostKey = Hasher.hash(host[1]);
				try {
					kvs.get("blacklist", hostKey, "");
					kvs.put("hosts", hostKey, "url", host[1]);
				} catch (Exception e) {
					e.printStackTrace();
				}
				int responseCode = 0;
				URL url = new URL(urlString);
				try {
					// Check if robot.txt has been requested for this host.
					byte[] robotBytes = kvs.get("hosts", hostKey, "robots.txt");
					if (robotBytes != null ) {
						// robots.txt has been requested
						String robot = new String(robotBytes);
						if (!robot.equals("FALSE")) {
							// This host has a robots.txt
							if (!parseRobotTxt(robot, host[3])) {
								// robots.txt forbids crawling of this page.
								return new ArrayList<String>();
							}
							// if robot.txt == "FALSE", we have already verified that this host does not have a robots.txt, so we can crawl freely.
						} 
					} else {
						//robot.txt has not been requested from this host. Request it.
						URL urlRobo = new URL(host[0] + "://" + host[1] + "/robots.txt");
						HttpURLConnection connRobo = (HttpURLConnection) urlRobo.openConnection();
						connRobo.setRequestProperty("User-Agent", "cis5550-crawler");
						connRobo.setRequestMethod("GET");
						connRobo.setConnectTimeout(10000);
						connRobo.connect();
						responseCode = connRobo.getResponseCode();
						if (responseCode != 200) {
							// The host has no robots.txt. Mark as FALSE so that future queries know.
							kvs.put("hosts", hostKey, "robots.txt", "FALSE");
						} else {
							// This host has a robots.txt
							String robot = readBody(connRobo);
							if (robot != null) {
								// Save robots.txt to KVS
								kvs.put("hosts", hostKey, "robots.txt", robot.getBytes());
								if (!parseRobotTxt(robot, host[3])) {
									// robots.txt forbids crawling of this page.
									return new ArrayList<String>();
								}
							} else {
								// The host has no robots.txt. Mark as FALSE so that future queries know.
								kvs.put("hosts", hostKey, "robots.txt", "FALSE");
							}
						}
					}
				} catch (UnknownHostException uhe) {
					return new ArrayList<String>();
				} catch (javax.net.ssl.SSLHandshakeException sslhse) {
					return new ArrayList<String>();
				} catch (Exception e) {
					System.out.println("Exception caused by " + urlString);
					e.printStackTrace();
					return new ArrayList<String>();
				}
				// Check last access time
				try {
					byte[] lastAccessTimeBytes = kvs.get("hosts", hostKey, "lastAccessTime");
					if (lastAccessTimeBytes != null) {
						long lastAccessTime = Long.parseLong(new String(lastAccessTimeBytes));
						if (System.currentTimeMillis() - lastAccessTime < 1000) {
							// If we have accessed this host within 1 second, put this page to the back of the queue to crawl later.
							return Arrays.asList(new String[] {urlString});
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					// If problems occur in checking last access time, crawl anyway???
					// return Arrays.asList(new String[] {urlString});
				}
				// Send a HEAD request with the URL
				HttpURLConnection connHead = (HttpURLConnection) url.openConnection();
				HttpURLConnection.setFollowRedirects(false);
				connHead.setRequestProperty("User-Agent", "cis5550-crawler");
				connHead.setRequestMethod("HEAD");
				connHead.setConnectTimeout(10000);
				// Update last access time
				try {
					kvs.put("hosts", hostKey, "lastAccessTime", String.valueOf(System.currentTimeMillis()));
					connHead.connect();
					// Register response code of HEAD request, also register meta-information of the page.
					responseCode = connHead.getResponseCode();
				} catch (UnknownHostException uhe) {
					return new ArrayList<String>();
				} catch (javax.net.ssl.SSLHandshakeException sslhse) {
					return new ArrayList<String>();
				} catch (Exception e) {
					System.out.println("Exception caused by " + urlString);
					e.printStackTrace();
					return new ArrayList<String>();
				}
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
					e.printStackTrace();
				}
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
				String contentType = connHead.getContentType();
				if (contentType == null) {
					kvs.putRow("crawl", row);
					return new ArrayList<String>();
				}
				if (responseCode != 200 || !contentType.trim().toLowerCase().startsWith("text/html")) {
					kvs.putRow("crawl", row);
					return new ArrayList<String>();
				}
				// See if we can filter out pages that are not in English
				String contentLanguage = connHead.getHeaderField("Content-Language");
				if (contentLanguage != null) {
					if (!contentLanguage.toLowerCase().contains("en")) {
						return new ArrayList<String>();
					}
				}
				// Send a GET request with the URL
				HttpURLConnection connGet = (HttpURLConnection) url.openConnection();
				connGet.setRequestProperty("User-Agent", "cis5550-crawler");
				connGet.setRequestMethod("GET");
				connHead.setConnectTimeout(10000);
				// Update access time.
				try {
					kvs.put("hosts", hostKey, "lastAccessTime", String.valueOf(System.currentTimeMillis()));
					connGet.connect();
				} catch (Exception e) {
					e.printStackTrace();
				}
				// Check again if the response code is 200 and if the content type is still text/html, even though it's unlikely that they have changed within such a short amount of time.
				responseCode = connGet.getResponseCode();
				row.put("responseCode",String.valueOf(responseCode));
				contentType = connGet.getContentType();
				if (contentType == null) {
					kvs.putRow("crawl", row);
					return new ArrayList<String>();
				}
				if (responseCode != 200 || !contentType.trim().toLowerCase().startsWith("text/html")) {
					return new ArrayList<String>();
				}
				// Finally, read the content of the page and put it to KVS.
				String contentStr = readBody(connGet);
				if (contentStr != null) {
					row.put("page", contentStr);
				}
				kvs.putRow("crawl", row);
				// Extract more URLs from this page and put them to the back of the queue.
				List<String> newURLs = extractURLs(contentStr, urlString, blacklist);
				return newURLs;
			});
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
	
	public static List<String> extractURLs(String content, String baseURL, List<String> blacklist) {
		System.out.println("Downloaded page: " + baseURL);
		if (content == null) {
			return new ArrayList<String>();
		}
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("<\\s*?a\\s+[^>]*href=\\s*?\"(.*?)\".*?>", java.util.regex.Pattern.CASE_INSENSITIVE);
		java.util.regex.Matcher matcher = pattern.matcher(content);
		List<String> newURLs = new ArrayList<String>();
	    while(matcher.find()) {
	    	String newURL = matcher.group(1);
	    	String newURLNorm = normalizeURL(baseURL, newURL);
	    	if (newURLNorm == null) {
	    		continue;
	    	}
	    	if (isBlacklisted(newURLNorm, blacklist)) {
	    		continue;
	    	}
	    	newURLs.add(newURLNorm);
	    }
	    return newURLs;
	}
	public static boolean isBlacklisted(String url, List<String> blacklist) {
        for (String b : blacklist) {
        	java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(b);
        	java.util.regex.Matcher matcher = pattern.matcher(url);
            if (matcher.find()) {
                return true;
            }
        }
        return false;
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
		if (!urlP[0].equals("http") && !urlP[0].equals("https")) {
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
