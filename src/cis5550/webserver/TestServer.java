package cis5550.webserver;

import static cis5550.webserver.Server.*;
import java.util.ArrayList;
import java.util.List;

import cis5550.webserver.Server.staticFiles;
import cis5550.webserver.*;

public class TestServer {
	public static void main(String args[]) throws Exception {
		port(8080);
//		staticFiles.location("frontend_interface/search-app/build");
		get("/", (req, res) -> {
			return Interface.homepage;
		});
		
		get("/search", (req, res) -> {
			
			String resp = "["
             + "{\"title\": \"title 1\", \"text\": \"http://simple.crawltest.cis5550.net:80/Cv1epgGc.html\"},"
             + "{\"title\": \"title 2\", \"text\": \"http://simple.crawltest.cis5550.net:80/ItU5tEu.html\"},"
             + "{\"title\": \"title 3\", \"text\": \"http://simple.crawltest.cis5550.net:80/LE4.html\"},"
             + "{\"title\": \"title 4\", \"text\": \"http://simple.crawltest.cis5550.net:80/\"}"
             + "]";

			res.header("Content-Type", "text/plain");
//			res.body(resp);

			res.write(resp.getBytes());
			return null;
		});
	}
	
	
}
