package cis5550.ranker;

import static cis5550.kvs.Row.readFrom;
import static java.lang.Math.min;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.jobs.Trie;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class CrawlProcess {
	
	 public static void main(String args[]) {
		 try {
			 RandomAccessFile crawl_refined = new RandomAccessFile("/Users/benedict/Desktop/Resources/CIS 555//holi-search-engine/worker1/crawl_refined.table", "rw");
			 RandomAccessFile file = new RandomAccessFile("/Users/benedict/Desktop/Resources/CIS 555/holi-search-engine/worker1/crawl.table", "rw");
	         while(true){
	             Row row = readFrom(file);
	             if(row == null)
	                 break;
	             String rowkey = row.key();
				 // Extract the title
				 String html_page = row.get("page");
				 if(html_page != null)
				 {
					 Pattern titlePattern = Pattern.compile("<title.*?>(.*?)</title>");
					 Matcher titleMatcher = titlePattern.matcher(html_page);
					 if (titleMatcher.find()) {
						 String title = titleMatcher.group(1);
						 title = title.substring(0, min(60, title.length()));
						 row.put("title", title);
					 }
					 // Extract the body
					 Pattern bodyPattern = Pattern.compile("<body.*?>(.*?)</body>", Pattern.DOTALL);
					 Matcher bodyMatcher = bodyPattern.matcher(html_page);
					 if (bodyMatcher.find()) {
						 String body = bodyMatcher.group(1);
						 body = body.replaceAll("\\<.*?\\>", " ");
						 body = body.replaceAll("[.,:;!?'\"()\\-\\p{Cntrl}]", " ");
						 body = body.substring(0, min(body.length(), 300));
						 row.put("snippet", body);
					 }
					 String default_text = html_page.replaceAll("\\<.*?\\>", " ")
							 .replaceAll("[.,:;!?'\"()\\-\\p{Cntrl}]", " ");
					 String default_body = default_text.substring(0, min(default_text.length(), 300));
					 String default_title = default_text.substring(0, min(default_text.length(), 60));
					 if(row.get("title") == null)
						 row.put("title", default_title);
					 if(row.get("snippet") == null)
						 row.put("snippet", default_body);l
				 }
				 crawl_refined.write(row.toByteArray());
				 crawl_refined.writeBytes("\n");
	         }
		 }
		 catch(Exception e) {
			 e.printStackTrace();
		 }
	 }
}