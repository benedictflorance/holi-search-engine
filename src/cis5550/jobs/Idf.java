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

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class Idf {
	public static void run(FlameContext ctx, String[] args) {
		try {
			
			FlameRDD flameRdd = ctx.fromTable("crawl", row -> row.get("url") + "," + row.get("page"));
			FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
			String masterAddr = ctx.getKVS().getMaster();
			
			Integer i =0;
			Iterator<Row> crawlRows = ctx.getKVS().scan("crawl");
			while(crawlRows.hasNext()){
				i++;
				crawlRows.next();
				
			}
			final Integer N =i;
			
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
	            
	            List<String> allWords = new ArrayList<String>();
	            for (String word : words) {
	            	if(!word.trim().isEmpty()) {
	            		//Word positions EC
	            		allWords.add(word);
	            	}
	            }
	           
	            //also added the stemmed version of all words
	            for (String word : words) {
	            	Stemmer s = new Stemmer();
	            	if(!word.trim().isEmpty()) {
	            		s.add(word.toCharArray(), word.length());
	            		s.stem();
	            		//Word positions EC
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
					
					if(kvs.get("index-EC", word, "acc")==null)
						continue;
					
					//Get the number of URLs from index table for that particular word
					Integer df = new String(kvs.get("index-EC", word, "acc")).split(",").length + 1;

//					N is the total number of keys in crawl.table
					Double idf = Math.log(1.0 * N / df);
					dfMap.put(word, df);
					idfMap.put(word, idf);
					
				}
	            Set<FlamePair> pairs = new HashSet<>();
	            for (Map.Entry<String, Integer> entry : dfMap.entrySet()) {
	                pairs.add(new FlamePair(entry.getKey(), String.valueOf(entry.getValue())));
	            }
	            
	            return new Iterable<FlamePair>() {
	                @Override
	                public Iterator<FlamePair> iterator()
	                {
	                    return pairs.iterator();
	                }
	            };
	        })
			.saveAsTable("df");		

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
	            
	            List<String> allWords = new ArrayList<String>();
	            for (String word : words) {
	            	if(!word.trim().isEmpty()) {
	            		//Word positions EC
	            		allWords.add(word);
	            	}
	            }
	           
	            //also added the stemmed version of all words
	            for (String word : words) {
	            	Stemmer s = new Stemmer();
	            	if(!word.trim().isEmpty()) {
	            		s.add(word.toCharArray(), word.length());
	            		s.stem();
	            		//Word positions EC
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
					//Get the number of URLs from index table for that particular word
					
					if(kvs.get("index-EC", word, "acc")==null)
						continue;
					Integer df = new String(kvs.get("index-EC", word, "acc")).split(",").length + 1;

//					N is the total number of keys in crawl.table
					Double idf = Math.log(1.0 * N / df);
					dfMap.put(word, df);
					idfMap.put(word, idf);
					
				}
				
	            Set<FlamePair> pairs = new HashSet<>();
	            for (Map.Entry<String, Double> entry : idfMap.entrySet()) {
	                pairs.add(new FlamePair(entry.getKey(), String.valueOf(entry.getValue())));
	            }
	            
	            return new Iterable<FlamePair>() {
	                @Override
	                public Iterator<FlamePair> iterator()
	                {
	                    return pairs.iterator();
	                }
	            };
	        })
			.saveAsTable("idf");	
			KVSClient kvs = new KVSClient(masterAddr);
//			kvs.persist("w-metric");
			Iterator<Row> tfRow = ctx.getKVS().scan("df");
			Iterator<Row> normalizedTfRow = ctx.getKVS().scan("idf");
			while(tfRow.hasNext()) {
				Row currRow = tfRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				kvs.put("w-metric", currRow.key(), "df", currRow.get(currCol.get(0)));
			}
			
			while(normalizedTfRow.hasNext()) {
				Row currRow = normalizedTfRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				kvs.put("w-metric", currRow.key(), "idf", currRow.get(currCol.get(0)));
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
