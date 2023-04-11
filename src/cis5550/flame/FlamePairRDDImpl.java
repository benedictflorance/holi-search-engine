package cis5550.flame;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
	
	private String tableName;
	public FlameContextImpl flameContextImpl = new FlameContextImpl(null);

	@Override
	public List<FlamePair> collect() throws Exception {
		KVSClient kvs = new KVSClient(Master.masterAddr);
		Iterator<Row> itr = kvs.scan(tableName);
		List<FlamePair> result = new ArrayList<FlamePair>();
		while (itr.hasNext()) {
			Row row = itr.next();
			for(String c: row.columns())
		    result.add(new FlamePair(row.key(),row.get(c)));
		}
		return result;
	}
	
	// foldByKey() folds all the values that are associated with a given key in the
	// current PairRDD, and returns a new PairRDD with the resulting keys and values.
	// Formally, the new PairRDD should contain a pair (k,v) for each distinct key k 
	// in the current PairRDD, where v is computed as follows: Let v_1,...,v_N be the 
	// values associated with k in the current PairRDD (in other words, the current 
	// PairRDD contains (k,v_1),(k,v_2),...,(k,v_N)). Then the provided lambda should 
	// be invoked once for each v_i, with that v_i as the second argument. The first
	// invocation should use 'zeroElement' as its first argument, and each subsequent
	// invocation should use the result of the previous one. v is the result of the
	// last invocation.
	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		String encodedZeroEle = URLEncoder.encode(zeroElement, StandardCharsets.UTF_8);
		String opTableName = (String) flameContextImpl.invokeOperation("/pairrdd/foldByKey",Serializer.objectToByteArray(lambda),tableName, encodedZeroEle);
		FlamePairRDDImpl resultRDD = new FlamePairRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	public void setTableName(String opTableName) {
		this.tableName = opTableName;
	}
	
	public String getTableName() {
		return tableName;
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		KVSClient kvs = new KVSClient(Master.masterAddr);
		kvs.rename(tableName,tableNameArg);
		setTableName(tableNameArg);
	}

	// flatMap() should invoke the provided lambda once for each pair in the PairRDD, 
	// and it should return a new RDD that contains all the strings from the Iterables 
	// the lambda invocations have returned. It is okay for the same string to appear 
	// more than once in the output; in this case, the RDD should contain multiple 
	// copies of that string. The lambda is allowed to return null or an empty Iterable.
	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/pairrdd/flatMap",Serializer.objectToByteArray(lambda),tableName, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	  // flatMapToPair() is analogous to flatMap(), except that the lambda returns pairs 
	  // instead of strings, and tha tthe output is a PairRDD instead of a normal RDD.
	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/pairrdd/flatMapToPair",Serializer.objectToByteArray(lambda),tableName, null);
		FlamePairRDDImpl resultRDD = new FlamePairRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	// join() joins the current PairRDD A with another PairRDD B. Suppose A contains
	// a pair (k,v_A) and B contains a pair (k,v_B). Then the result should contain
	// a pair (k,v_A+","+v_B).
	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/pairrdd/join",(((FlamePairRDDImpl)other).getTableName()).getBytes(),tableName, null);
		FlamePairRDDImpl resultRDD = new FlamePairRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}
	
	// This method should return a new PairRDD that contains, for each key k that exists 
	// in either the original RDD or in R, a pair (k,"[X],[Y]"), where X and Y are 
	// comma-separated lists of the values from the original RDD and from R, respectively. 
	// For instance, if the original RDD contains (fruit,apple) and (fruit,banana) and 
	// R contains (fruit,cherry), (fruit,date) and (fruit,fig), the result should contain 
	// a pair with key fruit and value [apple,banana],[cherry,date,fig]. This method is 
	// extra credit in HW7; if you do not implement it, please return 'null'.
	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		String opTableName = (String) flameContextImpl.invokeOperation("/pairrdd/cogroup",(((FlamePairRDDImpl)other).getTableName()).getBytes(),tableName, null);
		FlamePairRDDImpl resultRDD = new FlamePairRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}
	
	@Override
	public void delete() throws Exception {
		KVSClient kvs = new KVSClient(Master.masterAddr);
		kvs.delete(tableName);
	}

}
