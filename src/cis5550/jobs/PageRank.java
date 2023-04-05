package cis5550.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.tools.URLParser;

public class PageRank {
	public static void run(FlameContext context, String[] args) throws Exception {
		if (args.length < 1) {
			context.output("Convergence threshold not provided.");
			return;
		}
		float t = Float.parseFloat(args[0]);
		FlameRDD crawlTable = context.fromTable("crawl",  row -> {
			String url = row.get("url");
			String page = row.get("page");
			List<String> urls = extractURLs(page, url);
			Collections.sort(urls);
			StringBuilder sb = new StringBuilder();
			sb.append(url);
			for (int i = 0; i < urls.size(); i++) {
				String u = urls.get(i);
				if (i > 0) {
					if (u.equals(urls.get(i - 1))) {
						continue;
					}
				}
				sb.append(",");
				sb.append(u);
			}
			return sb.toString();
		});
		FlamePairRDD pageRank = crawlTable.mapToPair(str -> {
			int comma = str.indexOf(",");
			if (comma < 0) {
				return new FlamePair(str, "1.0,1.0,");
			}
			return new FlamePair(str.substring(0, comma), "1.0,1.0," + str.substring(comma + 1));
		});
		String floatFormat = "%f";
		while (true) {
			FlamePairRDD transfer = pageRank.flatMapToPair(pair -> {
				String[] adj = pair._2().split(",");
				java.util.List<FlamePair> ret = new java.util.ArrayList<FlamePair>();
				ret.add(new FlamePair(pair._1(), "0.0"));
				if (adj.length <= 2) {
					return ret;
				}
				float currRank = Float.parseFloat(adj[0]);
				float rcn = 0.85f * (currRank / (float) (adj.length - 2));
				ret.add(new FlamePair(pair._1(), "0.0"));
				for (int i = 2; i < adj.length; i++) {
					ret.add(new FlamePair(adj[i], String.format(floatFormat, rcn)));
				}
				return ret;
			});
			transfer = transfer.foldByKey("", (a, b) -> {
				if (a.length() == 0) {
					return b;
				}
				return String.format(floatFormat, Float.parseFloat(a) + Float.parseFloat(b));
			});
			pageRank = transfer.join(pageRank);
			pageRank = pageRank.flatMapToPair(pair -> {
				String v = pair._2();
				int comma0 = v.indexOf(",");
				float newR = Float.parseFloat(v.substring(0, comma0)) + 0.15f;
				int comma1 = v.indexOf(",", comma0 + 1);
				float curr = Float.parseFloat(v.substring(comma0 + 1, comma1));
				int comma2 = v.indexOf(",", comma1 + 1);
				String l = v.substring(comma2 + 1);
				FlamePair ret = new FlamePair(pair._1(), String.format(floatFormat, newR) + "," + String.format(floatFormat, curr) + "," + l);
				List<FlamePair> list = new ArrayList<FlamePair>();
				list.add(ret);
				return list;
			});
			FlameRDD convergence = pageRank.flatMap(pair -> {
				String v = pair._2();
				int comma0 = v.indexOf(",");
				float curr = Float.parseFloat(v.substring(0, comma0));
				int comma1 = v.indexOf(",", comma0 + 1);
				float prev = Float.parseFloat(v.substring(comma0 + 1, comma1));
				String change = String.format(floatFormat, Math.abs(prev - curr));
				List<String> ret = new ArrayList<>();
				ret.add(change);
				return ret;
			});
			String maxChange = convergence.fold("", (a, b) -> {
				if (a.length() == 0) {
					return b;
				}
				float currMax = Float.parseFloat(a);
				float val = Float.parseFloat(b);
				if (val > currMax) {
					return b;
				}
				return a;
			});
			float maxCh = Float.parseFloat(maxChange);
			if (maxCh < t) {
				break;
			}
			
		}
		String kvsMasterAddr = context.getKVS().getMaster();
		pageRank.flatMapToPair(pair -> {
			KVSClient kvs = new KVSClient(kvsMasterAddr);
			String k = pair._1();
			String v = pair._2();
			int comma = v.indexOf(",");
			String rank = v.substring(0, comma);
			kvs.put("pageranksmy", k, "rank", rank);
			return new ArrayList<>();
		});
		context.output("OK");
		
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
			return baseP[0] + "://" + baseP[1] + ":" + baseP[2] + cut(baseP[3], urlP[3]) + urlP[3].substring(0, end);
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
	
	public static String cut(String base, String url) {
		base = cutToLastSlash(base);
		int dotdot = url.indexOf("../");
		while (dotdot >= 0) {
			base = cutToLastSlash(base);
			dotdot = url.indexOf("../", dotdot + 3);
		}
		return null;
	}
	
	public static String cutToLastSlash(String url) {
		int i = url.length() - 1;
		while (url.charAt(i) != '/') {
			i--;
		}
		return url.substring(0, i + 1);
	}
}
