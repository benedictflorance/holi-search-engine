package cis5550.flame;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.kvs.*;

class Worker extends cis5550.generic.Worker {
	static ReentrantLock lock = new ReentrantLock();

	public static void main(String args[]) throws Exception {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <masterIP:port>");
    	System.exit(1);
    }
    
    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    
    post("/rdd/flatMap", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	
    	byte[] lambda = request.bodyAsBytes();
    	
    	StringToIterable deserializedLambda = (StringToIterable) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			Iterable<String> col = deserializedLambda.op(row.get("value"));
    			if(col!=null) {
    				Iterator<String> colItr = col.iterator();
    				while(colItr.hasNext()) {
    					kvs.put(outputTableName,Hasher.hash(UUID.randomUUID().toString()) ,"value", colItr.next());
    				}
    			}
    		}
    	}
    	return "OK";
      });
    
    
    post("/rdd/mapToPair", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	
    	byte[] lambda = request.bodyAsBytes();
    	
    	StringToPair deserializedLambda = (StringToPair) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			for(String c: row.columns()) {
	    			FlamePair flamePair = deserializedLambda.op(row.get(c));
	    			if(flamePair!=null) {
	    				kvs.put(outputTableName,flamePair.a ,row.key(), flamePair.b);
	    			}
    			}
    		}
    	}
    	return "OK";
      });
    
    post("/pairrdd/foldByKey", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	String encodedZeroEle = request.queryParams("encodedZeroEle");
    	String zeroEle = null;
    	if(encodedZeroEle!=null) {
    		zeroEle = URLDecoder.decode(encodedZeroEle,StandardCharsets.UTF_8);
    	}
        	
    	byte[] lambda = request.bodyAsBytes();
    	
    	TwoStringsToString deserializedLambda = (TwoStringsToString) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;
    	
    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
   
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			String accumulatedResult = zeroEle;
    			for(String c: row.columns()) {
    				if (c.equals("pos")) {
    					continue;
    				}
    				accumulatedResult = deserializedLambda.op(accumulatedResult,row.get(c));
    			}
    			kvs.put(outputTableName,row.key() ,"col", accumulatedResult);
    		}
    	}
    	return "OK";
      });
    
    
    post("/rdd/intersection-intermediate", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			kvs.put(outputTableName, URLEncoder.encode(row.get("value"), StandardCharsets.UTF_8) ,"value", row.get("value"));
    		}
    	}
    	return "OK";
      });
    
    post("/rdd/intersection", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	byte[] lambda = request.bodyAsBytes();
    	String targetTableName = new String(lambda);
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;
    	
    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	Iterator<Row> scannedTargetTable = null;
    	
    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, fromKey, null);
    	else
    		scannedTargetTable = kvs.scan(targetTableName, fromKey, toKey);
    	
    	
    	List<String> elements = new ArrayList<String>();
    	if(scannedTargetTable!=null) {
    		while(scannedTargetTable.hasNext()) {
	    		Row row = scannedTargetTable.next();
	    		elements.add(row.get("value"));
    		}
    	}
    	
    	//find intersection
    	Set<String> result = new HashSet<String>();
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			if(elements.contains(row.get("value"))) {
    				result.add(row.get("value"));
    			}
    		}
    	}
    	
    	for(String r:result) {
    		kvs.put(outputTableName,Hasher.hash(UUID.randomUUID().toString()) ,"value", r);
    	}
    	
    	return "OK";
      });
    
    post("/rdd/groupBy", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	
    	byte[] lambda = request.bodyAsBytes();
    	
    	StringToString deserializedLambda = (StringToString) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);

    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
	    		String newRowKey = deserializedLambda.op(row.get("value"));
    			kvs.put(outputTableName,newRowKey,Hasher.hash(UUID.randomUUID().toString()), row.get("value"));
    		}
    	}   	
    	return "OK";
      });
    
    
    post("/rdd/groupByAggregate", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);

    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			String value="";
    			for(String c: row.columns()) {
    				value = value + "," + row.get(c);
    			}
    			value = value.substring(1);
    			kvs.put(outputTableName,row.key(),"col", value);
    		}
    	}

    	return "OK";
      }); 
    
    post("/rdd/sample", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	byte[] lambda = request.bodyAsBytes();
    	double f = ByteBuffer.wrap(lambda).getDouble();
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);

    	List<String> result = new ArrayList<String>();
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			if (Math.random() < f) {
    	           result.add(row.get("value"));
    	        }
    		}
    	}
    	
    	for(String r:result) {
    		kvs.put(outputTableName,Hasher.hash(UUID.randomUUID().toString()) ,"value", r);
    	}
    	
    	return "OK";
      });
    
    post("/rdd/fromTable", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	
    	byte[] lambda = request.bodyAsBytes();
    	
    	RowToString deserializedLambda = (RowToString) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			String resultRow = deserializedLambda.op(row);
    			if (resultRow == null || resultRow.equals("null")) {
    				continue;
    			}
    			kvs.put(outputTableName,Hasher.hash(UUID.randomUUID().toString()) ,"value", resultRow);
    		}
    	}
    	return "OK";
      });
    
    post("/pairrdd/flatMap", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	
    	byte[] lambda = request.bodyAsBytes();
    	
    	PairToStringIterable deserializedLambda = (PairToStringIterable) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			
    			for(String c: row.columns()) {
    				Iterable<String> flamePairString = deserializedLambda.op(new FlamePair(row.key(),row.get(c)));
    				if(flamePairString!=null) {
	    				for(String s:flamePairString) {
	    					kvs.put(outputTableName,Hasher.hash(UUID.randomUUID().toString()),"value", s);
	    				}
    				}
    				
    			}
    		}
    	}
    	return "OK";
      });
    
    post("/pairrdd/flatMapToPair", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	
    	byte[] lambda = request.bodyAsBytes();
    	
    	PairToPairIterable deserializedLambda = (PairToPairIterable) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			
    			for(String c: row.columns()) {
    				Iterable<FlamePair> flamePairItr = deserializedLambda.op(new FlamePair(row.key(),row.get(c)));
    				if(flamePairItr!=null) {
	    				for(FlamePair fp:flamePairItr) {
	    					kvs.put(outputTableName,fp._1(),Hasher.hash(UUID.randomUUID().toString()), fp._2());
	    				}
    				}
    			}
    		}
    	}
    	return "OK";
      });
    
    post("/rdd/flatMapToPair", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	
    	byte[] lambda = request.bodyAsBytes();
    	
    	StringToPairIterable deserializedLambda = (StringToPairIterable) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			Iterable<FlamePair> flamePairItr = deserializedLambda.op(row.get("value"));
    			if(flamePairItr!=null) {
	    			for(FlamePair fp:flamePairItr) {
	    					kvs.put(outputTableName,fp._1(),Hasher.hash(UUID.randomUUID().toString()), fp._2());
	    				}
	    			}
    			}
    		}
    	return "OK";
      });
    
    post("/rdd/distinct", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			kvs.put(outputTableName, URLEncoder.encode(row.get("value"), StandardCharsets.UTF_8) ,"value", row.get("value"));
    		}
    	}
    	return "OK";
      });
    
    post("/pairrdd/join", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	byte[] lambda = request.bodyAsBytes();
    	String targetTableName = new String(lambda);
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	Iterator<Row> scannedTargetTable = null;
    	
    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, fromKey, null);
    	else
    		scannedTargetTable = kvs.scan(targetTableName, fromKey, toKey);

    	HashSet<String> rowKeySet = new HashSet<>();
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			rowKeySet.add(row.key());
    		}
    	} 
    	
    	if(scannedTargetTable!=null) {
    		while(scannedTargetTable.hasNext()) {
    			Row row = scannedTargetTable.next();
    			if(rowKeySet.contains(row.key())) {
    				Set<String> col1 = row.columns();
    				Set<String> col2 = kvs.getRow(inputTableName,row.key()).columns();
    				for(String c1:col1) {
    					for(String c2:col2) {
    						kvs.put(outputTableName,row.key(),c1 + "+" + c2, new String(kvs.get(inputTableName,row.key(),c2)) + "," + row.get(c1) );
    					}
    				}
    			}
    		}
    	} 
    	return "OK";
      });
    
    post("/rdd/fold", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	String encodedZeroEle = request.queryParams("encodedZeroEle");
    	String zeroEle = null;
    	if(encodedZeroEle!=null) {
    		zeroEle = URLDecoder.decode(encodedZeroEle,StandardCharsets.UTF_8);
    	}
        	
    	byte[] lambda = request.bodyAsBytes();
    	
    	TwoStringsToString deserializedLambda = (TwoStringsToString) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;
    	
    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			String accumulatedResult = zeroEle;
    			for(String c: row.columns()) {
    				if (c.equals("pos")) {
    					continue;
    				}
    				accumulatedResult = deserializedLambda.op(accumulatedResult, row.get(c));
    			}
    			//ensuring that the accumulated result of all workers go to the same work during the aggregate step
    			kvs.put(outputTableName,"rowPlaceholder1234567890" + row.key() ,"value", accumulatedResult);
    		}
    	}
    	return "OK";
      });
    
    post("/rdd/foldAggregate", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	String encodedZeroEle = request.queryParams("encodedZeroEle");
    	String zeroEle = null;
    	if(encodedZeroEle!=null) {
    		zeroEle = URLDecoder.decode(encodedZeroEle,StandardCharsets.UTF_8);
    	}
        	
    	byte[] lambda = request.bodyAsBytes();
    	
    	TwoStringsToString deserializedLambda = (TwoStringsToString) Serializer.byteArrayToObject(lambda, myJAR);
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;
    	
    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	
    	String accumulatedResult = zeroEle;
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			accumulatedResult = deserializedLambda.op(accumulatedResult, row.get("value"));
    		}
    		if(!accumulatedResult.equals(zeroEle))
    			kvs.put(outputTableName ,"opRow", "value", accumulatedResult);
    	}
    	return "OK";
      });
    
    post("/rdd/filter", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	byte[] lambda = request.bodyAsBytes();
    	StringToBoolean deserializedLambda = (StringToBoolean) Serializer.byteArrayToObject(lambda, myJAR);
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);

    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			if(deserializedLambda.op(row.get("value")))
    				kvs.put(outputTableName,Hasher.hash(UUID.randomUUID().toString()) ,"value", row.get("value"));
    		}
    	}
    	
    	return "OK";
      });
    
    post("/rdd/mapPartitions", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	byte[] lambda = request.bodyAsBytes();
    	IteratorToIterator deserializedLambda = (IteratorToIterator) Serializer.byteArrayToObject(lambda, myJAR);
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);

    	ArrayList<String> list = new ArrayList<>();
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			list.add(row.get("value"));
    		}
    	}
    	
    	Iterator<String> result = deserializedLambda.op(list.iterator());
    	if(result!=null) {
    		while(result.hasNext())
    			kvs.put(outputTableName,Hasher.hash(UUID.randomUUID().toString()) ,"value", result.next());
    	}
    	
    	return "OK";
      });
    
    post("/pairrdd/cogroup", (request,response) -> {
    	String inputTableName = request.queryParams("input");
    	String outputTableName = request.queryParams("output");
    	String fromKey = request.queryParams("fromKey");
    	String toKey = request.queryParams("toKey");
    	String masterAddr = request.queryParams("masterAddr");
    	byte[] lambda = request.bodyAsBytes();
    	String targetTableName = new String(lambda);
    	
    	KVSClient kvs = new KVSClient(masterAddr);
    	
    	Iterator<Row> scannedTable = null;

    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTable = kvs.scan(inputTableName, fromKey, null);
    	else
    		scannedTable = kvs.scan(inputTableName, fromKey, toKey);
    	
    	Iterator<Row> scannedTargetTable = null;
    	
    	if(fromKey.equals("null") && toKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, null, null);
    	else if(fromKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, null, toKey);
    	else if(toKey.equals("null"))
    		scannedTargetTable = kvs.scan(targetTableName, fromKey, null);
    	else
    		scannedTargetTable = kvs.scan(targetTableName, fromKey, toKey);

    	HashSet<String> rowKeySet = new HashSet<>();
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			rowKeySet.add(row.key());
    		}
    	} 
    	
    	if(scannedTargetTable!=null) {
    		while(scannedTargetTable.hasNext()) {
    			Row row = scannedTargetTable.next();
    			//for keys that are in intersection
    			if(rowKeySet.contains(row.key())) {
    				Set<String> col1 = kvs.getRow(inputTableName,row.key()).columns();
    				Set<String> col2 = row.columns();
    				
    				String result1 = "[";
    				for(String c1: col1) {
    					result1+=new String(kvs.get(inputTableName,row.key(),c1)) + ",";
    				}
    				result1 = result1.substring(0,result1.length()-1);
    				result1+="]";
    				String result2 = "[";
    				for(String c2: col2) {
    					result2+=row.get(c2) + ",";
    				}
    				result2 = result2.substring(0,result2.length()-1);
    				result2+="]";
    				kvs.put(outputTableName,row.key(),Hasher.hash(UUID.randomUUID().toString()), result1 + "," + result2);
    			}
    			//for keys in target table, RDD2, that are not in intersection
    			else {
    				Set<String> col = row.columns();
    				String result = "[";
    				for(String c: col) {
    					result+=row.get(c) + ",";
    				}
    				result = result.substring(0,result.length()-1);
    				result+="]";
    				kvs.put(outputTableName,row.key(),Hasher.hash(UUID.randomUUID().toString()), "[]," + result);
    			}
    		}
    	} 
    	//for keys in input table, RDD 1, that are not in intersection
    	for(String r: rowKeySet) {
    		if(kvs.getRow(outputTableName, r)==null) {
    			Set<String> col = kvs.getRow(inputTableName, r).columns();
				String result = "[";
				for(String c: col) {
					result+= new String(kvs.get(inputTableName, r, c)) + ",";
				}
				result = result.substring(0,result.length()-1);
				result+="]";
				kvs.put(outputTableName,r,Hasher.hash(UUID.randomUUID().toString()),result+",[]");
    		}
    	}
    	return "OK";
      });
    
	}
}
