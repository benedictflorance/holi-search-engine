package cis5550.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class RobotsTxtParser {
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
	
	public static boolean robotPermits(String hostKey, String[] urlParts, String wholeURL, KVSClient kvs, Row host, Row row) {
		try {
			int responseCode;
			// Check if robot.txt has been requested for this host.
			String robot = host.get("robots.txt");
			if (robot != null ) {
				// robots.txt has been requested
				if (robot.equals("FALSE") || robot.equals("IGNORE")) {
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
				host.put("robots.txt", "FALSE");
				kvs.put("hosts", hostKey, "robots.txt", "FALSE");
				return true;
			}
			int roboLength = connRobo.getContentLength();
			if (roboLength > 1024 * 1024 * 512) {
				host.put("robots.txt", "FALSE");
				kvs.put("hosts", hostKey, "robots.txt", "FALSE");
				return true;
			}
			// This host has a robots.txt
			String robots = readBody(connRobo);
			if (robots == null) {
				// The host has no robots.txt. Mark as FALSE so that future queries know.
				host.put("robots.txt", "FALSE");
				kvs.put("hosts", hostKey, "robots.txt", "FALSE");
				return true;
			}
			// Save robots.txt to KVS
			host.put("robots.txt", robots);
			if (!parseRobotsTxt(robot, urlParts[3])) {
				// robots.txt forbids crawling of this page
				return false;
			}
			kvs.put("hosts", hostKey, "robots.txt", robots.getBytes());
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
	
}
