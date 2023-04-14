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
			// query the ranker 
			String urlStr = "http://" + rankerAddr + "/search?q=" + URLEncoder.encode(req.queryParams("q"), StandardCharsets.UTF_8);
    		URL url = new URL(urlStr);
    		// trigger a HTTP request
    		url.getContent();
    		
    		InputStream in = url.openStream();
    		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    		
    		String resp = "";
    		String line = null;
    		while ((line = reader.readLine()) != null) {
    			resp += line;
    		}
			
//			String resp = "["
//             + "{\"title\": \"title 1\", \"text\": \"http://simple.crawltest.cis5550.net:80/Cv1epgGc.html\"},"
//             + "{\"title\": \"title 2\", \"text\": \"http://simple.crawltest.cis5550.net:80/ItU5tEu.html\"},"
//             + "{\"title\": \"title 3\", \"text\": \"http://simple.crawltest.cis5550.net:80/LE4.html\"},"
//             + "{\"title\": \"title 4\", \"text\": \"http://simple.crawltest.cis5550.net:80/\"}"
//             + "]";
    		
			res.header("Content-Type", "text/plain");

			res.write(resp.getBytes());
			return null;
		});
	}
	
	
}
