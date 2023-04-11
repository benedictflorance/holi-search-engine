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

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class TermFrequency {
	
	public static void run(FlameContext ctx, String[] args) {
		try {
			
			FlameRDD flameRdd = ctx.fromTable("crawl", row -> row.get("url") + "," + row.get("page"));
			FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
			flamePairRdd.flatMapToPair(urlPage -> {
				
	            String url = urlPage._1();
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

	            // Split into words
	            String[] words = page.split("\\s+");
	            
	            // Create (word, url) pairs with positions
	            Map<String, Set<Integer>> wordPositions = new ConcurrentHashMap<>();
	            
	            // TODO: check - Remove duplicates
//	            Set<String> uniqueWords = new HashSet<>(Arrays.asList(words));
	            
	            
	            int pos = 1;
	            for (String word : words) {
	            	if(!word.trim().isEmpty()) {
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
	                pairs.add(new FlamePair(Hasher.hash(url) + ":" + entry.getKey(), String.valueOf(entry.getValue())));
	            }
	            
	            return new Iterable<FlamePair>() {
	                @Override
	                public Iterator<FlamePair> iterator()
	                {
	                    return pairs.iterator();
	                }
	            };
	        })
			.saveAsTable("tf-temp");	

			flameRdd = ctx.fromTable("crawl", row -> row.get("url") + "," + row.get("page"));
			flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
			flamePairRdd.flatMapToPair(urlPage -> {
				
	            String url = urlPage._1();
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

	            // Split into words
	            String[] words = page.split("\\s+");
	            
	            // Create (word, url) pairs with positions
	            Map<String, Set<Integer>> wordPositions = new ConcurrentHashMap<>();
	            
	            // TODO: check - Remove duplicates
//	            Set<String> uniqueWords = new HashSet<>(Arrays.asList(words));
	            
	            
	            int pos = 1;
	            for (String word : words) {
	            	if(!word.trim().isEmpty()) {
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
	            for (Map.Entry<String, Double> entry : normalizedTfMap.entrySet()) {
	                pairs.add(new FlamePair(Hasher.hash(url) + ":" + entry.getKey(), String.valueOf(entry.getValue())));
	            }
	            
	            return new Iterable<FlamePair>() {
	                @Override
	                public Iterator<FlamePair> iterator()
	                {
	                    return pairs.iterator();
	                }
	            };
	        })
			.saveAsTable("normalized-tf-temp");	
			
			ctx.getKVS().persist("wd-metric");
			Iterator<Row> tfRow = ctx.getKVS().scan("tf-temp");
			Iterator<Row> normalizedTfRow = ctx.getKVS().scan("normalized-tf-temp");
			while(tfRow.hasNext()) {
				Row currRow = tfRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				ctx.getKVS().put("wd-metric", currRow.key(), "tf", currRow.get(currCol.get(0)));
			}
			
			while(normalizedTfRow.hasNext()) {
				Row currRow = normalizedTfRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				ctx.getKVS().put("wd-metric", currRow.key(), "normalized-tf", currRow.get(currCol.get(0)));
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
