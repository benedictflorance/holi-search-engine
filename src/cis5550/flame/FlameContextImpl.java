package cis5550.flame;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext{
	
	private String outputBody="";
	private Boolean outputCalled;
	public String jarName;
	KVSClient kvs;
	Vector<WorkerEntry> kvsWorkers;
	private int keyRangesPerWorker = -1;

	public FlameContextImpl(String jarName) {
		this.jarName = jarName;
		kvsWorkers = new Vector<WorkerEntry>();
		kvs = new KVSClient(Master.masterAddr);
	}
	
	static class WorkerEntry implements Comparable<WorkerEntry> {
		String address;
		String id;

		WorkerEntry(String addressArg, String idArg) {
		  address = addressArg;
		  id = idArg;
		}

		public int compareTo(WorkerEntry e) {
		   return id.compareTo(e.id);
		}
	};
	
	synchronized void downloadWorkers() throws IOException {
	    String result = new String(HTTP.doRequest("GET", "http://"+kvs.getMaster()+"/workers", null).body());
	    String[] pieces = result.split("\n");
	    int numWorkers = Integer.parseInt(pieces[0]);
	    if (numWorkers < 1)
	      throw new IOException("No active KVS workers");
	    if (pieces.length != (numWorkers+1))
	      throw new RuntimeException("Received truncated response when asking KVS master for list of workers");
	    kvsWorkers.clear();
	    for (int i=0; i<numWorkers; i++) {
	      String[] pcs = pieces[1+i].split(",");
	      kvsWorkers.add(new WorkerEntry(pcs[1], pcs[0]));
	    }
	    Collections.sort(kvsWorkers);
	  }
	
	public Object invokeOperation(String nameOfOp, byte[] lambda, String inputTable, String encodedZeroEle) throws Exception {
		String opTableName = String.valueOf(System.currentTimeMillis()) + UUID.randomUUID();
		kvs.persist(opTableName);
		
		downloadWorkers();
		Partitioner partitioner = new Partitioner();
		if(keyRangesPerWorker!=-1) {
			partitioner.setKeyRangesPerWorker(keyRangesPerWorker);
		}
		
		for (int index=0; index<kvsWorkers.size(); index++) {
			if(index==kvsWorkers.size()-1) {
				partitioner.addKVSWorker(kvsWorkers.get(index).address,kvsWorkers.get(index).id, null);
				partitioner.addKVSWorker(kvsWorkers.get(index).address,null, kvsWorkers.get(0).id);
			}
			else {
				partitioner.addKVSWorker(kvsWorkers.get(index).address, kvsWorkers.get(index).id, kvsWorkers.get(index+1).id);
			}
		}
		
		Vector<String> flameWorkers = cis5550.generic.Master.getWorkers();
		for (int i=0; i<flameWorkers.size(); i++) {
			partitioner.addFlameWorker(flameWorkers.elementAt(i));
		}
		
		Vector<Partition> partitionVec = partitioner.assignPartitions();
		
	     Thread threads[] = new Thread[partitionVec.size()];
	     String results[] = new String[partitionVec.size()];
	     System.out.println("Partition of task done: " + partitionVec.size());
	     for (int i=0; i<partitionVec.size(); i++) {
	        final String url = "http://"+partitionVec.get(i).assignedFlameWorker+nameOfOp+"?input="+inputTable+"&output="+opTableName+
	        		"&fromKey="+partitionVec.get(i).fromKey+"&toKey="+partitionVec.get(i).toKeyExclusive+"&masterAddr="+Master.masterAddr
	        		+"&encodedZeroEle="+encodedZeroEle;
	        
	 
	        System.out.println(url);
	        final int j = i;
	        threads[i] = new Thread("Worker #"+(i+1)) {
	          public void run() {
	            try {
	              results[j] = new String(HTTP.doRequest("POST", url, lambda).body());
	            } catch (Exception e) {
	              results[j] = "Exception: "+e;
	              e.printStackTrace();
	            }
	          }
	        };
	        threads[i].start();
	      }

	      // Wait for all the uploads to finish
	      for (int i=0; i<threads.length; i++) {
	        try {
	          threads[i].join();
	        } catch (InterruptedException ie) {
	        }
	      }
	      
//	      for(int i=0; i<results.length; i++) {
//	    	  if(!results[i].equals("OK")){
//            	  //throw a custom exception
//	    		  throw new CustomException("Worker "+ i + " failed and returned " + results[i]);
//              }
//	      }
	      
	      return opTableName;
	}

	@Override
	public KVSClient getKVS() {
		return kvs;
	}

	// When a job invokes output(), your solution should store the provided string
	// and return it in the body of the /submit response, if and when the job 
	// terminates normally. If a job invokes output() more than once, the strings
	// should be concatenated. If a job never invokes output(), the body of the
	// /submit response should contain a message saying that there was no output.
	@Override
	public void output(String s) {
		setOutputCalled(true);
		setOutputBody(s);
	}

	// This function should return a FlameRDD that contains the strings in the provided
	// List. It is okay for this method to run directly on the master; it does not
	// need to be parallelized.
	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		String tableName = String.valueOf(System.currentTimeMillis()) + UUID.randomUUID();
		
		FlameRDDImpl flameRDD = new FlameRDDImpl();
		flameRDD.setTableName(tableName);
		
		Integer i = 1;
		for(String l: list) {
			kvs.put(tableName, Hasher.hash(i.toString()), "value", l);
			i++;
		}
		return flameRDD;
	}

	public String getOutputBody() {
		return outputBody;
	}

	public void setOutputBody(String outputBody) {
		this.outputBody += outputBody;
	}

	public Boolean getOutputCalled() {
		return outputCalled;
	}

	public void setOutputCalled(Boolean outputCalled) {
		this.outputCalled = outputCalled;
	}
	
	  // This function should scan the table in the key-value store with the specified name, 
	  // invoke the provided lambda with each Row of data from the KVS, and then return
	  // and RDD with all the strings that the lambda invocations returned. The lambda
	  // is allowed to return null for certain Rows; when it does, no data should be
	  // added to the RDD for these Rows. This method should run in parallel on all the
	  // workers, just like the RDD/PairRDD operations.
	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		String opTableName = (String) invokeOperation("/rdd/fromTable", Serializer.objectToByteArray(lambda), tableName, null);
		FlameRDDImpl resultRDD = new FlameRDDImpl();
		resultRDD.setTableName(opTableName);
		return resultRDD;
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		this.keyRangesPerWorker = keyRangesPerWorker;
	}

}
