package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class Idf {
	public static void run(FlameContext ctx, String[] args) {
		try {
			//TODO: change names of crawl and index tables!!
			
			
			FlameRDD flameRdd = ctx.fromTable("crawl-190", row -> row.get("url") + "," + row.get("page"));
			FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
			String masterAddr = ctx.getKVS().getMaster();
			
			Integer i =0;
			Iterator<Row> crawlRows = ctx.getKVS().scan("crawl-190");
			while(crawlRows.hasNext()){
				i++;
				crawlRows.next();
				
			}
			final Integer N =i;
			
			flamePairRdd.flatMapToPair(urlPage -> {
				
				String url = urlPage._1();
	            System.out.println(url);
	            String page = urlPage._2();
	            
	            if(url==null || page==null)
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
	            
//	            // Cut the page size into 1/5th
//	            page = page.substring(0, page.length()/5);
	            
	            // Remove punctuation
	            page = page.replaceAll("[.,:;!?'\"\\(\\)-]", " ");
	            
	            //Remove non alpha numeric characters
	            page = page.replaceAll("[^a-zA-Z0-9]", " ");

	            //Remove non ASCII characters
	            page = page.replaceAll("[^\\p{ASCII}]", " ");

	            // Split into words
	            String[] words = page.split("\\s+");
	            Trie trie = new Trie();
				trie.buildTrie("src/cis5550/jobs/words_alpha.txt");
	            
	            List<String> allWords = new ArrayList<String>();
	            for (String word : words) {
	            	if(!word.trim().isEmpty()) {
		            	word = word.trim();
		            	if(!trie.containsWord(word)) {
		            		System.out.println("Not an English word: " + word);
		            		continue;
		            	}
		            		
		            	word = word.toLowerCase();
	            		allWords.add(word);
	            	}
	            }
	           
	            //also added the stemmed version of all words
	            for (String word : words) {
	            	Stemmer s = new Stemmer();
	            	if(!word.trim().isEmpty()) {
	            		word = word.trim();
	            		s.add(word.toCharArray(), word.length());
	            		s.stem();
	            		word = word.toLowerCase();
	            		allWords.add(s.toString());
	            	}
	            }
	            KVSClient kvs = new KVSClient(masterAddr);
				Map<String, Integer> dfMap = new ConcurrentHashMap<>();
				Map<String, Double> idfMap = new ConcurrentHashMap<>();
				for (String word: allWords) {

					// Compute inverse document frequency
					//The inverse document frequency is calculated by dividing the total number of documents 
					// by the number of documents in which the word appears, and then taking the logarithm of this value.
					if(word.trim().isEmpty())
						continue;
					
					if(kvs.get("index-190", word, "url")==null)
						continue;
					
					//Get the number of URLs from index table for that particular word
					Integer df = new String(kvs.get("index-190", word, "url")).split(",").length + 1;

//					N is the total number of keys in crawl.table
					//TODO: check base
					Double idf = Math.log(1.0 * N / df);
					dfMap.put(word, df);
					idfMap.put(word, idf);
					
				}
	            Set<FlamePair> pairs = new HashSet<>();
	            for (Map.Entry<String, Integer> entry : dfMap.entrySet()) {
	            	//df + idf
	                pairs.add(new FlamePair(entry.getKey(), String.valueOf(entry.getValue()) +
	                		"+" +  String.valueOf(idfMap.get(entry.getKey()))));
	            }
	            
	            return new Iterable<FlamePair>() {
	                @Override
	                public Iterator<FlamePair> iterator()
	                {
	                    return pairs.iterator();
	                }
	            };
	        })
			.saveAsTable("temp-1");			
			
			KVSClient kvs = new KVSClient(masterAddr);
			kvs.persist("w-metric");
			Iterator<Row> tfRow = ctx.getKVS().scan("temp-1");
			while(tfRow.hasNext()) {
				Row currRow = tfRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				ctx.getKVS().put("w-metric", currRow.key(), "df", currRow.get(currCol.get(0)).split("\\+")[0]);
				ctx.getKVS().put("w-metric", currRow.key(), "idf", currRow.get(currCol.get(0)).split("\\+")[1]);
			}
			
			ctx.output("OK");
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
