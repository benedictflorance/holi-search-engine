package cis5550.jobs;

import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class URLExtractor {

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
	
	public static Set<String> extractURLs(String content, String baseURL, List<String> blacklist, KVSClient kvs, boolean checkDup) {
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
	    	if (countChar(newURLNorm, '/') > 5) {
	    		continue;
	    	}
	    	String urlHash  = Hasher.hash(newURLNorm);
	    	if (checkDup) {
	    		if (URLCrawled(urlHash, kvs)) {
	    			continue;
	    		}
	    	}
	    	newURLs.add(newURLNorm);
	    }
	    return newURLs;
	}
	
	public static int countChar(String url, char c) {
		int count = 0;
		for (int i = 0; i < url.length(); i++) {
			if (url.charAt(i) == c) {
				count++;
			}
		}
		return count;
	}
	
	public static String normalizeURL(String base, String url) {
		// Parse results:
		// 0: http or https
		// 1: host name
		// 2: port
		// 3: uri
		String[] baseP = URLParser.parseURL(base);
		String[] urlP = URLParser.parseURL(url);
		if (baseP[1] != null) {
			if (!baseP[1].startsWith("www.")) {
				baseP[1] = "www." + baseP[1];
			}
		}
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
		if (!urlP[1].startsWith("www.")) {
			urlP[1] = "www." + urlP[1];
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
		if (uri.contains("@") || uri.contains("&") || uri.contains("{") || uri.contains("}") || uri.contains("=") || uri.contains("?") || uri.contains("+")) {
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
	
	public static String[] buildBadURLsList() {
		String proto = "http.*:\\/\\/";
		return new String[] {"\\/cgi-bin\\/",
							 "\\/javascript\\/",
							 "\\.appfinders\\.com",
							  "[www.]*youtube\\.com",
							 "[www.]*flickr\\.com",
							 "\\/weather[?|/]",
							 "\\/play[?|/]",
							 "\\/reel[?|/]",
							 "\\/calendar[?|/]",
							 "\\/sounds[?|/]",
							 "\\/iplayer[?|/]",
							 "\\/sounds[?|/]",
							 "\\/bin[?|/]",
							 "\\/search",
							 "\\/video",
							 "\\/watch[?|/]",
							 "email",
							 "login",
							 "signup",
							 "\\/ads[?|/]"
							 };
	}
	
	public static boolean isBlacklisted(String url, List<String> blacklist) {
		if(blacklist!=null) {
	        for (String b : blacklist) {
	        	Pattern pattern = Pattern.compile(b);
	        	Matcher matcher = pattern.matcher(url);
	            if (matcher.find()) {
	                return true;
	            }
	        }
		}
        return false;
    }
	
	public static void main(String[] args) {
		System.out.println(isBlacklisted("https://www.web.com/ads?outddd=2", java.util.Arrays.asList(buildBadURLsList())));
	}
}
