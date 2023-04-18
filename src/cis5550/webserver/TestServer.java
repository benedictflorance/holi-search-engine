package cis5550.webserver;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import cis5550.webserver.Server.staticFiles;
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
		
//		staticFiles.location("frontend_interface/search-app/build");
		get("/", (req, res) -> {
			return Interface.homepage;
		});
		
		get("/search", (req, res) -> {
			int currentPage = 1;
			String pQueryParam = req.queryParams("p");
			if (pQueryParam != null && pQueryParam.matches("\\d+")) {
			    currentPage = Integer.parseInt(pQueryParam);
			    System.out.print(currentPage);
			}
			res.header("Content-Type", "text/plain");

			
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
    		

			return null;
		});
		
		
	}
	
}
	

