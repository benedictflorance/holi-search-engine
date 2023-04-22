package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class Idf {
	
	public static String CRAWL = "crawl-1316";
	public static String INDEX = "index";
	public static String W_METRIC = "w-metric";
	public static void run(FlameContext ctx, String[] args) {
		try {
			
			Integer i =0;
			Iterator<Row> crawlRows = ctx.getKVS().scan(CRAWL);
			while(crawlRows.hasNext()){
				i++;
				crawlRows.next();
				
			}
			final Integer N =i;
			
			FlameRDD flameRdd = ctx.fromTable(INDEX, row -> row.key() + "," + row.get("url"));
			FlamePairRDD flamePairRdd = flameRdd.mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",",2)[1]));
			
			String masterAddr = ctx.getKVS().getMaster();
			
			flamePairRdd.flatMapToPair(indexUrl -> {
				
				String index_word = indexUrl._1();
	            System.out.println(index_word);
	            String urls = indexUrl._2();
	            
	            Integer df = urls.split(",").length + 1;
	            Double idf = Math.log(1.0 * N / df);

	            Set<FlamePair> pairs = new HashSet<>();
	            //df + idf
	            pairs.add(new FlamePair(index_word, String.valueOf(df) +
	                		"+" +  String.valueOf(idf)));
	            
	           return pairs;
	        })
			.saveAsTable("temp-1");			
			
			KVSClient kvs = new KVSClient(masterAddr);
			kvs.persist(W_METRIC);
			Iterator<Row> dfRow = kvs.scan("temp-1");
			while(dfRow.hasNext()) {
				Row currRow = dfRow.next();
				List<String> currCol = new ArrayList<String>(currRow.columns());
				for(String c: currCol) {
					if(c.equals("pos"))
						continue;
					kvs.put(W_METRIC, currRow.key(), "df", currRow.get(c).split("\\+")[0]);
					kvs.put(W_METRIC, currRow.key(), "idf", currRow.get(c).split("\\+")[1]);
				}
			}
			
			ctx.getKVS().delete("temp-1");
			
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
