package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
//import org.apache.commons.text.WordUtils;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;


public class Indexer {
	
	public static void run(FlameContext ctx, String[] args) {
		try {
			
			FlameRDD flameRdd = ctx.fromTable("crawl", row -> row.get("url") + "," + row.get("page"));
	             
			FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
			flamePairRdd.flatMapToPair(urlPage -> {
				
	            String url = urlPage._1();
	            System.out.println(url);
	            String page = urlPage._2();
	            
	            if(url==null || page==null)
	            	return null;

	            // Remove HTML tags
	            page = page.replaceAll("<.*?>", " ");
	            //convert to lowercase
	            page = page.toLowerCase();
	            // Remove punctuation
//	            page = page.replaceAll("[^a-z\\s]", "");
	            page = page.replaceAll("[.,:;!?'\"\\(\\)-]", " ");
	            
	            //Remove non alpha numeric characters
	            page = page.replaceAll("[^a-zA-Z0-9]", " ");

	            //Remove non ASCII characters
	            page = page.replaceAll("[^\\p{ASCII}]", " ");

	            // Split into words
	            String[] words = page.split("\\s+");
	            
	            // Create (word, url) pairs with positions
	            Map<String, Set<Integer>> wordPositions = new ConcurrentHashMap<>();
	            
	            // TODO: check - Remove duplicates
//	            Set<String> uniqueWords = new HashSet<>(Arrays.asList(words));
	            
	            
	            int pos = 1;
	            for (String word : words) {
	            	if(!word.trim().isEmpty()) {
//	            		pairs.add(new FlamePair(word.trim(), url));
	            		
	            		//Word positions EC
	            		wordPositions.putIfAbsent(word,new TreeSet<>());
	            		wordPositions.get(word).add(pos);
	            		pos++;
	            	}
	            }
	           
	            pos = 1;
	            //also added the stemmed version of all words
	            for (String word : words) {
	            	Stemmer s = new Stemmer();
	            	if(!word.trim().isEmpty()) {
	            		s.add(word.toCharArray(), word.length());
	            		s.stem();
//	            		pairs.add(new FlamePair(s.toString(), url));
	            		
	            		//Word positions EC
	            		wordPositions.putIfAbsent(s.toString(),new TreeSet<>());
	            		wordPositions.get(s.toString()).add(pos);
	            		
	            		pos++;
	            	}
	            }
	            
	            // Create (word, url) pairs
	            Set<FlamePair> pairs = new HashSet<>();
	            for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) {
	                String word = entry.getKey();
	                Set<Integer> positions = entry.getValue();
	                StringBuilder sb = new StringBuilder();
	                for (Integer position : positions) {
	                	sb.append(position).append(" ");
	                }
	                String result = sb.toString().trim();
	                pairs.add(new FlamePair(word, url + ":" + result));
	            }
	            
	            
	            
	            return new Iterable<FlamePair>() {
	                @Override
	                public Iterator<FlamePair> iterator()
	                {
	                    return pairs.iterator();
	                }
	            };
	        })
			.foldByKey("", (u1, u2) -> {
			    if (u1.isEmpty()) {
			        return u2;
			    } else if (u2.isEmpty()) {
			        return u1;
			    } else {
			        List<String> urlList = new ArrayList<>(Arrays.asList((u1 + "," + u2).split(",")));
			        
			        // Flatten the list of URLs into a single string
			        String urlsString = String.join(", ", urlList);
			        
			        // Split the string of URLs by commas and zero or more spaces
			        String[] urls = urlsString.split(",\\s*");
			        
			        // Sort urls based on how many spaces each one has, in descending order
			        Arrays.sort(urls, new Comparator<String>() {
			            public int compare(String url1, String url2) {
			                int spaceCount1 = url1.length() - url1.replaceAll("\\s+", "").length();
			                int spaceCount2 = url2.length() - url2.replaceAll("\\s+", "").length();
			                return spaceCount2 - spaceCount1;
			            }
			        });
			        return String.join(",", urls);
			    }
			})
			.saveAsTable("index-temp");
			
			Iterator<Row> indexRow = ctx.getKVS().scan("index-temp");
			while(indexRow.hasNext()) {
				Row currRow = indexRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				ctx.getKVS().put("index", currRow.key(), "url", currRow.get(currCol.get(0)));
			}
			
			ctx.output("OK");
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			ctx.output("Exception");
			e.printStackTrace();
		} catch (IOException e) {
			ctx.output("Exception");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			ctx.output("Exception");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}