package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;

public class PageRank {
	public static void run(FlameContext ctx, String[] args) {
		
		FlameRDD flameRdd;
		String masterAddr = ctx.getKVS().getMaster();
		
		try {
			flameRdd = ctx.fromTable("crawl-1316", row -> {
				String url = row.get("url");
				String page = row.get("page");
				if (page == null) {
					return null;
				}
				Set<String> urls = URLExtractor.extractURLs(page, url, Constants.blacklist, new KVSClient(masterAddr), false);
				StringBuilder sb = new StringBuilder();
				sb.append(url);
				for (String u : urls) {
					sb.append(",");
					sb.append(u);
				}
				return sb.toString();
			});
			
			FlamePairRDD stateTable = flameRdd.mapToPair(s -> { 
				
				int comma = s.indexOf(",");
				if (comma < 0) {
					// If this URL has 0 out-degree.
					return new FlamePair(s, "1.0,1.0,");
				}
				return new FlamePair(s.substring(0, comma), "1.0,1.0," + s.substring(comma + 1));
			});
			
			String maxChange = "1.0";
			Double convergenceThreshold;
			if(args.length>=1)
				convergenceThreshold = Double.parseDouble(args[0]);
			else convergenceThreshold = 0.01;
			
			Double decayFactor = 0.85;
			Integer numberOfIterations = 0;
			while(true) {
				numberOfIterations++;
				FlamePairRDD transferTable = stateTable
						.flatMapToPair(pair -> {
						    String[] tokens = pair._2().split(",");
						    String[] urls = Arrays.copyOfRange(tokens, 2, tokens.length);
						    //check for duplicate links
						    Set<String> set = new LinkedHashSet<>(Arrays.asList(urls));
						    List<FlamePair> results = new ArrayList<>();
						    urls = set.toArray(new String[0]);
						    int n = urls.length;
						    if(n==0) {
						    	results.add(new FlamePair(pair._1(),"0.0"));
						    	return results;
						    }
						    Double rc = Double.parseDouble(tokens[0]);
						    System.out.println("rc: " + rc);
//						    Double v = decayFactor * rc / n;
						    Double v = rc / n;
						    System.out.println("v: " + v);
						    
						    Boolean selfLink = false;
							for (int i = 0; i < urls.length; i++) {
							   	// check for self link
							    if (urls[i].equals(pair._1()))
							    	selfLink = true;
							    results.add(new FlamePair(urls[i], String.valueOf(v)));	
							}
						    // add self-loop with rank 0.0 to prevent vertexes with indegree zero from disappearing
						    if(!selfLink)
						    	results.add(new FlamePair(pair._1(),"0.0"));
						    
						    return new Iterable<FlamePair>() {
				                @Override
				                public Iterator<FlamePair> iterator()
				                {
				                    return results.iterator();
				                }
				            };
						})
						.foldByKey("0.0",(a,b) -> ""+(Double.parseDouble(a)+Double.parseDouble(b)));

				FlamePairRDD newStateTable = stateTable
					    .join(transferTable)
					    .flatMapToPair(t -> {
					    	
					    try {
					        String url = t._1();
					        String[] fields = t._2().split(",");
					        Double currentRank = Double.parseDouble(fields[0]);
					        Double previousRank = Double.parseDouble(fields[1]);
					        previousRank = currentRank;
//					        currentRank = (1 - decayFactor) + Double.parseDouble(fields[fields.length-1]);
					        currentRank = (1 - decayFactor) + (decayFactor * Double.parseDouble(fields[fields.length-1]));
					        String outLinks = String.join(",",  Arrays.copyOfRange(fields, 2, fields.length-1));
					        List<FlamePair> pairs = new ArrayList<>();
					        pairs.add(new FlamePair(url, currentRank + "," + previousRank + "," + outLinks));
					        return pairs;
					    }
					    catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    return null;
				});
				
				// Compute the maximum change in ranks across all pages
			    maxChange = newStateTable
			            .flatMap(pair -> {
			                String[] fields = pair._2().split(",");
			                double currentRank = Double.parseDouble(fields[0]);
			                double previousRank = Double.parseDouble(fields[1]);
//			                System.out.println("Current Rank: " + currentRank + ", Previous Rank: " + previousRank);
			                
			                return new Iterable<String>() {
				                @Override
				                public Iterator<String> iterator()
				                {
				                    return Collections.singletonList(String.valueOf(Math.abs(currentRank - previousRank))).iterator();
				                }
				            };
			            })
			            .fold("0.0", (a, b) -> ""+Math.max(Double.parseDouble(a), Double.parseDouble(b)));
			    
			    System.out.println("maxChange" + maxChange);
			    
			    transferTable.delete();
			    stateTable.delete();
			    // Replace old state table with new one
			    stateTable = newStateTable;
			    
			    // Check for convergence
			    if (Double.parseDouble(maxChange) < convergenceThreshold) {
			        break;
			    }
			    
				
			}
			
			KVSClient kvs1 = new KVSClient(masterAddr);
			kvs1.persist("pageranks");
			stateTable.flatMapToPair(t -> {
			    String url = t._1();
			    String[] fields = t._2().split(",");
			    Double currentRank = Double.parseDouble(fields[0]);
			    KVSClient kvs = new KVSClient(masterAddr);
			    kvs.put("pageranks",url, "rank", String.valueOf(currentRank));
			    return Collections.emptyList();
			});
			
			System.out.println("Number of Iterations:" + numberOfIterations);
			ctx.output("OK");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
