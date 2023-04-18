package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
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
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class TermFrequency {
	
	public static String CRAWL = "crawl-678";
	public static String INDEX = "index-678";
	public static String WD_METRIC = "wd-metric";
	
	public static void run(FlameContext ctx, String[] args) {
		try {
			
			FlameRDD flameRdd = ctx.fromTable(CRAWL, row -> row.get("url") + "," + row.get("page"));
			FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
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
		            //convert to lowercase
//		            page = page.toLowerCase();
		            
		            // Remove punctuation
		            page = page.replaceAll("[.,:;!?'\"\\(\\)-]", " ");
		            
		            //Remove non alpha numeric characters
		            page = page.replaceAll("[^a-zA-Z0-9]", " ");

		            //Remove non ASCII characters
		            page = page.replaceAll("[^\\p{ASCII}]", " ");
		            
//			         // Cut the page size into half
//		            page = page.substring(0, page.length()/2);

		            // Split into words
		            String[] words = page.split("\\s+");
		            
		            // Create (word, url) pairs with positions
		            Map<String, Set<Integer>> wordPositions = new ConcurrentHashMap<>();
		            
		            // TODO: check - Remove duplicates
//		            Set<String> uniqueWords = new HashSet<>(Arrays.asList(words));
		            
		            Trie trie = new Trie();
					trie.buildTrie("cis5550/jobs/words_alpha.txt");
		            int pos = 1;
		            for (String word : words) {
		            	if(!word.trim().isEmpty()) {
		            	if (word.length() > 512) {
			            	continue;
			            }	
		            	word = word.trim();
		            	if(!trie.containsWord(word)) {
		            		System.out.println("Not an English word: " + word);
		            		continue;
		            	}
		            		
		            	word = word.toLowerCase();
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
	            		s.add(word.toCharArray(), word.length());
	            		s.stem();
	            		word = word.toLowerCase();
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
	            
	         // Compute term frequency (tf) and inverse document frequency (idf)
				Map<String, Integer> tfMap = new ConcurrentHashMap<>();
				Map<String, Double> normalizedTfMap = new ConcurrentHashMap<>();
				for (Map.Entry<String, Set<Integer>> entry : wordPositions.entrySet()) {
					String word = entry.getKey();
					Set<Integer> positions = entry.getValue();

					// Compute term frequency
					Double normalizedTf = positions.size() / l2Norm;
					
					tfMap.put(word,positions.size());
					normalizedTfMap.put(word, normalizedTf);
				}
	            Set<FlamePair> pairs = new HashSet<>();
	            for (Map.Entry<String, Integer> entry : tfMap.entrySet()) {
	            	//put tf + normalizedTf
	            	if(entry.getKey()==null)
	            		continue;
	                pairs.add(new FlamePair(Hasher.hash(url) + ":" + entry.getKey(), String.valueOf(entry.getValue()) +
	                		"+" +  String.valueOf(normalizedTfMap.get(entry.getKey()))));
	            }
	            
	            return new Iterable<FlamePair>() {
	                @Override
	                public Iterator<FlamePair> iterator()
	                {
	                    return pairs.iterator();
	                }
	            };
	        })
			.saveAsTable("temp");	
			
			ctx.getKVS().persist(WD_METRIC);
			Iterator<Row> tfRow = ctx.getKVS().scan("temp");
			while(tfRow.hasNext()) {
				Row currRow = tfRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				ctx.getKVS().put(WD_METRIC, currRow.key(), "tf", currRow.get(currCol.get(0)).split("\\+")[0]);
				ctx.getKVS().put(WD_METRIC, currRow.key(), "normalized-tf", currRow.get(currCol.get(0)).split("\\+")[1]);
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
