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
			flameRdd = ctx.fromTable("crawl-678", row -> row.get("url") + "," + row.get("page"));
			
			FlamePairRDD stateTable = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0],
					"1.0,1.0," + extractUrls(s.split(",")[0], s.split(",",2)[1])));
			
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
						    urls = set.toArray(new String[0]);
						    
//						    int n = tokens.length - 2;
						    int n = urls.length;
						    Double rc = Double.parseDouble(tokens[0]);
//						    Double v = decayFactor * rc / n;
						    Double v = rc / n;
						    
						    List<FlamePair> results = new ArrayList<>();
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
			    
			    
			    
			    if(args.length>1 && args[1]!=null) {
			    	Double convergencePercentage = Double.parseDouble(args[1])/100;
				    List<FlamePair> maxChangeList = newStateTable
				            .flatMapToPair(pair -> {
				                String[] fields = pair._2().split(",");
				                double currentRank = Double.parseDouble(fields[0]);
				                double previousRank = Double.parseDouble(fields[1]);
				                Set<FlamePair> pairs = new HashSet<>();
					            pairs.add(new FlamePair(pair._1(), String.valueOf(Math.abs(currentRank - previousRank))));
					            return new Iterable<FlamePair>() {
					                @Override
					                public Iterator<FlamePair> iterator()
					                {
					                    return pairs.iterator();
					                }
					            };
				            }).collect();
				    Integer noOfUrls = maxChangeList.size();
				    Integer noOfUrlsConverged = 0;
				    for(FlamePair fp: maxChangeList){
				    	if(Double.parseDouble(fp._2())<convergenceThreshold) {
				    		noOfUrlsConverged++;
				    	}
				    }
				    int result = (Double.valueOf(convergencePercentage*noOfUrls)).compareTo(noOfUrlsConverged.doubleValue());
				    if(result<0) {
				    	break;
				    }
			    }
			    
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
	
	private static String extractUrls(String baseUrl, String pageContent){
			
			List<String> urls = new ArrayList<>();
	//	    Pattern pattern = Pattern.compile("<a\\s+[^>]*href=\"([^\"]+)\"[^>]*>", Pattern.CASE_INSENSITIVE);
		    Pattern pattern = Pattern.compile("<a\\s+[^>]*href=[\"']([^\"']*)[\"'][^>]*>(.*?)</a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		    
		    Matcher matcher = pattern.matcher(pageContent);
		    while (matcher.find()) {
		        try {
		        	String url = matcher.group(1);
		        	String normalizedUrl = 	UrlNormalizer.normalize(baseUrl, url);
		        	
					if(normalizedUrl!=null) { 
						if(normalizedUrl.contains(".."))
							continue;
						if(endsWithUnwanted(removeParams(normalizedUrl))) {
			        		continue;
			        	}
						urls.add(removeParams(normalizedUrl));
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		    

		    String extractedUrls = String.join(",", urls);
		    return extractedUrls;
		}
	
	public static boolean endsWithUnwanted(String uri) {
		// String[] unwantedList = {".jpg", ".jpeg", ".gif", ".png", ".txt", ".pdf", ".aspx", ".asp", ".cfml", ".cfm", ".js", ".css", ".c", ".cpp", ".cc", ".java", ".sh", ".obj", ".o", ".h", ".hpp", ".json", ".env", ".class", ".php", ".php3", ".py"};
		// java.util.Set<String> unwanted = new java.util.HashSet<String>();
		if (uri.length() == 0) {
			return false;
		}
		if (uri.contains("@")) {
			return true;
		}
		if (uri.contains("javascript:")) {
			return true;
		}
		if (!uri.contains(".")) {
			return false;
		}
		// If the url contains a . and ends not with .html, we don't want the uri
		if (!uri.endsWith(".html")) {
			return true;
		}
		return false;
	}
	
	public static String removeParams(String uri) {
		int i = 0;
		while (i < uri.length()) {
			if (uri.charAt(i) == '#' || uri.charAt(i) == '?' || uri.charAt(i) == '=') {
				break;
			}
			i++;
		}
		return uri.substring(0, i);
	}

}
