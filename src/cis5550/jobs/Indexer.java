package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;


public class Indexer {
	
	public static void run(FlameContext ctx, String[] args) {
		try {
			
			final String masterAddr = ctx.getKVS().getMaster();
			ctx.getKVS().persist("ntf");
			
			FlameRDD flameRdd = ctx.fromTable("crawl", row -> {
				String page = row.get("page");
				if(page!=null) {
					String result = row.get("url") + "," + row.get("page");
					return result;
				}
				return null;
				});
	             
			FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
			flamePairRdd.flatMapToPair(true, urlPage -> {
				try {
				
		            String url = urlPage._1();
		            System.out.println(url);
		            String page = urlPage._2();
	
		            if(url==null || url.equals("null") || page==null || page.equals("null"))
		            	return null;
	       
		         	// Remove content from meta, script and link tags
		            String patternString = "<(meta|script|link)(\\s[^>]*)?>.*?</(meta|script|link)>";
		            // Compile the pattern
		            Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		            // Match the pattern against the HTML string
		            Matcher matcher = pattern.matcher(page);
		            page = matcher.replaceAll(" ");
		            
		            // Remove HTML tags
		            page = page.replaceAll("<.*?>", " ");
		            
		            // Remove punctuation
		            page = page.replaceAll("[.,:;!?'\"\\(\\)-]", " ");
		            
		            //Remove non alpha numeric characters
		            page = page.replaceAll("[^a-zA-Z0-9]", " ");
	
		            //Remove non ASCII characters
		            page = page.replaceAll("[^\\p{ASCII}]", " ");
		            
//		            // Cut the page size into half
//		            page = page.substring(0, page.length()/2);
	
		            // Split into words
		            String[] words = page.split("\\s+");
		            
		            // Create (word, url) pairs with positions
		            Map<String, Set<Integer>> wordPositions = new ConcurrentHashMap<>();
		            
		            Trie trie = new Trie();
					trie.buildTrie("cis5550/jobs/words_alpha.txt");
		            int pos = 1;
		            for (String word : words) {
	            		if (word.length() > 512) {
	            			continue;
	            		}
		            	if(!word.trim().isEmpty()) {
		            		word = word.trim();
	//	            		if(!new EnglishWordChecker().isEnglishWord(word)) {
	//		            		System.out.println("Not an English word: " + word);
	//		            		continue;
	//		            	}
		            		if(!trie.containsWord(word)) {
		            			continue;
		            		}
		            		
		            		word = word.toLowerCase();
		            		
		            		//Word positions EC
		            		wordPositions.putIfAbsent(word,new TreeSet<>());
		            		wordPositions.get(word).add(pos);
		            		pos++;
		            	}
		            }
		           
		            pos = 1;
		            //also added the stemmed version of all words
		            for (String word : words) {
		            	if (word.length() > 512) {
		            		continue;
		            	}
		            	Stemmer s = new Stemmer();
		            	if(!word.trim().isEmpty()) {
		            		word = word.trim();
		            		word = word.toLowerCase();
		            		s.add(word.toCharArray(), word.length());
		            		s.stem();
		            		
		            		//Word positions EC
		            		wordPositions.putIfAbsent(s.toString(),new TreeSet<>());
		            		wordPositions.get(s.toString()).add(pos);
		            		
		            		pos++;
		            	}
		            }
		            
		            //compute L2 norm over all document level term frequencies
		            Double l2Norm = 0.0;
		        	for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) {
		        		Integer wordTf = entry.getValue().size();
		        		l2Norm+=(wordTf*wordTf);
		        	}
		        	l2Norm = Math.sqrt(l2Norm);
		        	
		        	KVSClient kvs = new KVSClient(masterAddr);
		        	kvs.put("ntf", url, "ntf",String.valueOf(Math.sqrt(l2Norm)));
		            
		            // Create (word, url) pairs
		            Set<FlamePair> pairs = new HashSet<>();
		            for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) {
		                String word = entry.getKey();
		                Set<Integer> positions = entry.getValue();
		                pairs.add(new FlamePair(word, url + ":" + positions.size()));
		            }
		            return pairs;
		            
				}
				catch(Exception e) {
					e.printStackTrace();
				}
				return null;
	        })
			.foldByKey("", (u1, u2) -> {
			    if (u1.isEmpty()) {
			        return u2;
			    } else if (u2.isEmpty()) {
			        return u1;
			    } else {
			        List<String> urlList = new ArrayList<>(Arrays.asList((u1 + "," + u2).split(",")));
			        return String.join(",", urlList);
			    }
			})
			.saveAsTable("index-temp");
			
			Iterator<Row> indexRow = ctx.getKVS().scan("index-temp");
			ctx.getKVS().persist("index");
			while(indexRow.hasNext()) {
				Row currRow = indexRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				ctx.getKVS().put("index", currRow.key(), "url", currRow.get(currCol.get(0)));
			}
			
			ctx.getKVS().delete("index-temp");
			
			ctx.output("OK");
			
		} catch (FileNotFoundException e) {
			ctx.output("Exception");
			e.printStackTrace();
		} catch (IOException e) {
			ctx.output("Exception");
			e.printStackTrace();
		}catch (Exception e) {
			ctx.output("Exception");
			e.printStackTrace();
		}
		
	}

}