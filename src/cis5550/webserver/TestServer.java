package cis5550.webserver;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import cis5550.webserver.Server.staticFiles;
import cis5550.jobs.Trie;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.webserver.*;

public class TestServer {
	public static void main(String args[]) throws Exception {
		// parse frontend server ip:port
		if (args.length < 1) {
			System.out.println("please provide port for frontend server and ip:port for ranker server");
			return;
		}
		
		port(Integer.parseInt(args[0]));
		System.out.println("webserver listening on port " + args[0] + " ...");
		
		// parse ranker server ip:port
		if (args.length < 2) {
			System.out.println("please provide ip:port for ranker server");
			return;
		}
		
		String rankerAddr = args[1];
		System.out.println("Ranker listening on address " + args[1] + " ...");
		
		// parse kvs master ip:port
		if (args.length < 3) {
			System.out.println("please provide ip:port for kvs master server");
			return;
		}
		
		String kvsAddr = args[2];
		KVSClient kvs = new KVSClient(args[2]);
		System.out.println("KVS Master listening on address " + args[2] + " ...");
		
		staticFiles.location("static");
		get("/", (req, res) -> {
			return Interface.home_cached;
		});
		
		get("/search", (req, res) -> {
			try {
				int currentPage = 1;
				String pQueryParam = req.queryParams("p");
				if (pQueryParam != null && pQueryParam.matches("\\d+")) {
				    currentPage = Integer.parseInt(pQueryParam);
				    System.out.print(currentPage);
				}
				res.header("Content-Type", "text/plain");

				System.out.println(pQueryParam);
				// query the ranker 
				String urlStr = "http://" + rankerAddr + "/search?q=" + URLEncoder.encode(req.queryParams("q"), StandardCharsets.UTF_8) + "&page=" + Integer.toString(currentPage);
				URL url = new URL(urlStr);

				// trigger a HTTP request and get the response
				InputStream in = url.openConnection().getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));

				// read the response and send it back to the client
				String resp = "";
				String line = null;
				while ((line = reader.readLine()) != null) {
				    resp += line;
				}
				
				res.write(resp.getBytes());
				return null;
			} catch (Exception e) {
				System.out.println("No result found.");
				return null;
			}
			
			
//			String resp = "["
//             + "{\"title\": \"title 1\", \"url\": \"http://simple.crawltest.cis5550.net:80/Cv1epgGc.html\"},"
//             + "{\"title\": \"title 2\", \"url\": \"http://simple.crawltest.cis5550.net:80/ItU5tEu.html\"},"
//             + "{\"title\": \"title 3\", \"url\": \"http://simple.crawltest.cis5550.net:80/LE4.html\"},"
//             + "{\"title\": \"title 4\", \"url\": \"http://simple.crawltest.cis5550.net:80/\"}"
//             + "]";
			
//			int resultsPerPage = 10;
//			int totalPages = 5;

//			for (int page = 1; page <= totalPages; page++) {
//				List<SearchResult> searchResults = new ArrayList<>();
//				for (int i = 1; i <= resultsPerPage; i++) {
//					String title = "Result " + ((page - 1) * resultsPerPage + i);
//					String url = "https://example.com/result/" + ((page-1)*resultsPerPage + i);
//					SearchResult temp = new SearchResult(title, url);
//					searchResults.add(temp);
//				}
//				SearchResultsResponse response = new SearchResultsResponse(searchResults, page, totalPages);
//				if (page == currentPage) {
//					String jsonResponse = new Gson().toJson(response);
//					System.out.println(jsonResponse);
//					return jsonResponse;
//				}
//			}
    		
		});
		
		get("/cached", (req, res) -> {
			String url = req.queryParams("url");
			System.out.println("get cached page for url: " + url);
			byte[] cachedPage = kvs.get("crawl_refined", Hasher.hash(url), "page");
			return new String(cachedPage);
		});
		
		get("/suggestion", (req, res) -> {
			String query = req.queryParams("word");
			System.out.println("query: " + query);
	    	Trie trie = new Trie();
	    	trie.buildTrie("cis5550/jobs/words_alpha.txt");
	    	List<String> stringList = new ArrayList<>(trie.getSuggestions(query));
//	    	Collections.shuffle(stringList);
	    	
	    	String jsonResponse = "";
	    	if (stringList.size() > 5) {
	    		jsonResponse = new Gson().toJson(stringList.subList(0, 5));
	    	} else {
	    		jsonResponse = new Gson().toJson(stringList);
	    	}

			System.out.println(jsonResponse);
			return jsonResponse;
		});
		
	}
	
}
	

