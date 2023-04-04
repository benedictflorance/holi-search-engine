package cis5550.flame;

import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
	
	private String tableName;
	public FlameContextImpl flameContextImpl = new FlameContextImpl(null);
	Boolean intersectionCalled = false;
	
	// collect() should return a list that contains all the elements 
	// in the RDD.
	@Override
	public List<String> collect() throws Exception {
		KVSClient kvs = new KVSClient(Master.masterAddr);
		Iterator<Row> itr = kvs.scan(tableName);
		List<String> result = new ArrayList<String>();
		while (itr.hasNext()) {
		    result.add(itr.next().get("value"));
		}
		return result;
	}
	
	// flatMap() should invoke the provided lambda once for each element 
	// of the RDD, and it should return a new RDD that contains all the
	// strings from the Iterables the lambda invocations have returned.
	// It is okay for the same string to appear more than once in the output;
	// in this case, the RDD should contain multiple copies of that string.
	// The lambda is allowed to return null or an empty Iterable.
	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/flatMap",Serializer.objectToByteArray(lambda),tableName, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}
	
	// mapToPair() should invoke the provided lambda once for each element
	// of the RDD, and should return a PairRDD that contains all the pairs
	// that the lambda returns. The lambda is allowed to return null, and
	// different invocations can return pairs with the same keys and/or the
    // same values.
	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/mapToPair",Serializer.objectToByteArray(lambda),tableName, null);
		FlamePairRDDImpl resultRDD = new FlamePairRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}
	
	// intersection() should return an RDD that contains only elements 
	// that are present both 1) in the RDD on which the method is invoked,
	// and 2) in the RDD that is given as an argument. The returned RDD
	// should contain each unique element only once, even if one or both
	// of the input RDDs contain multiple instances. This method is extra
	// credit on HW6 and should return 'null' if this EC is not implemented.
	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		String op1 = (String) flameContextImpl.invokeOperation("/rdd/intersection-intermediate",null,tableName, null);
		String op2 = (String) flameContextImpl.invokeOperation("/rdd/intersection-intermediate",null,((FlameRDDImpl)r).getTableName(), null);
		
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/intersection",op2.getBytes(),op1, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	// sample() should return a new RDD that contains each element in the 
	// original RDD with the probability that is given as an argument.
	// If the original RDD contains multiple instances of the same element,
	// each instance should be sampled individually. This method is extra
	// credit on HW6 and should return 'null' if this EC is not implemented.
	@Override
	public FlameRDD sample(double f) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/sample",ByteBuffer.allocate(8).putDouble(f).array(),tableName, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	// groupBy() should apply the given lambda to each element in the RDD
	// and return a PairRDD with elements (k, V), where k is a string that
	// the lambda returned for at least one input element and V is a
	// comma-separated list of elements in the original RDD for which the
	// lambda returned k. This method is extra credit on HW6 and should 
	// return 'null' if this EC is not implemented.
	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		String op1 = (String) flameContextImpl.invokeOperation("/rdd/groupBy",Serializer.objectToByteArray(lambda),tableName, null);
		
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/groupByAggregate",null,op1, null);
		FlamePairRDDImpl resultRDD = new FlamePairRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public int count() throws Exception {
		KVSClient kvs = new KVSClient(Master.masterAddr);
		return kvs.count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		KVSClient kvs = new KVSClient(Master.masterAddr);
		kvs.rename(tableName,tableNameArg);
		setTableName(tableNameArg);
	}

	// distinct() should return a new RDD that contains the same
	// elements, except that, if the current RDD contains multiple
	// copies of some elements, the new RDD should contain only 
	// one copy of those elements.
	@Override
	public FlameRDD distinct() throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/distinct",null,tableName, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		Vector<String> result = new Vector<String>();
		KVSClient kvs = new KVSClient(Master.masterAddr);
		Iterator<Row> rows = kvs.scan(tableName);
		if(rows!=null) {
    		while(rows.hasNext() && num!=0) {
    			num--;
    			Row row = rows.next();
    			result.add(row.get("value"));
    			
    		}
    	}
		return result;
	}

	
	// fold() should call the provided lambda for each element of the 
	// RDD, with that element as the second argument. In the first
	// invocation, the first argument should be 'zeroElement'; in
	// each subsequent invocation, the first argument should be the
	// result of the previous invocation. The function returns
	// the result of the last invocation, or 'zeroElement' if the 
	// RDD does not contain any elements.
	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		String encodedZeroEle = URLEncoder.encode(zeroElement, StandardCharsets.UTF_8);
		String op1 = (String) flameContextImpl.invokeOperation("/rdd/fold",Serializer.objectToByteArray(lambda),tableName, encodedZeroEle);
		
		KVSClient kvs = new KVSClient(Master.masterAddr);
    	Iterator<Row> scannedTable = null;
    	scannedTable = kvs.scan(op1, null, null);
    	String accumulatedResult = zeroElement;
    	if(scannedTable!=null) {
    		while(scannedTable.hasNext()) {
    			Row row = scannedTable.next();
    			accumulatedResult = lambda.op(row.get("value"),accumulatedResult);
    		}
    	}
    	return accumulatedResult;
    	
//		String outputTableName = (String) flameContextImpl.invokeOperation("/rdd/foldAggregate",Serializer.objectToByteArray(lambda),op1, encodedZeroEle);
//		KVSClient kvs = new KVSClient(Master.masterAddr);
//		return new String(kvs.get(op1, "opRow", "value"));
    	
	}

	
	// flatMap() should invoke the provided lambda once for each element 
	// of the RDD, and it should return a new RDD that contains all the
	// strings from the Iterables the lambda invocations have returned.
	// It is okay for the same string to appear more than once in the output;
	// in this case, the RDD should contain multiple copies of that string.
	// The lambda is allowed to return null or an empty Iterable.
	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/flatMapToPair",Serializer.objectToByteArray(lambda),tableName, null);
		FlamePairRDDImpl resultRDD = new FlamePairRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	
	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/filter",Serializer.objectToByteArray(lambda),tableName, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/rdd/mapPartitions",Serializer.objectToByteArray(lambda),tableName, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

}
