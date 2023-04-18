package cis5550.kvs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.webserver.Server;
import cis5550.webserver.ThreadPool.Task;

public class PersistentTable implements Table {
	Map<String, Long> index;
	RandomAccessFile log;
	File tableFile;
	String id;
	String dir;
	
	public PersistentTable(String tKey, String dir) throws IOException {
		this.index = new ConcurrentHashMap<String, Long>();
		this.tableFile = new File(dir + "/" + tKey + ".table");
		this.tableFile.createNewFile();
		this.log = new RandomAccessFile(tableFile, "rws");
		this.id = tKey;
		this.dir = dir;
	}

	public PersistentTable(String tKey, String dir, File logFile) throws Exception {
		index = new ConcurrentHashMap<String, Long>();
		this.tableFile = logFile;
		this.log = new RandomAccessFile(tableFile, "rws");
		this.id = tKey;
		this.dir = dir;
		recover();
	}
	
	public PersistentTable(String tKey, String dir, Map<String, Row> data) throws Exception {
		this(tKey, dir);
		this.index = new ConcurrentHashMap<String, Long>();
		for (String rKey : data.keySet()) {
			putRow(rKey, data.get(rKey));
		}
		
	}
	
	private synchronized void recover() throws Exception {
		try {
			while (log.getFilePointer() != log.length()) {
				long offset = log.getFilePointer();
				Row r = Row.readFrom(log);
				index.put(r.key, offset);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized void putRow(String rKey, Row row) throws Exception {
		try {
			long offset = log.length();
			log.seek(offset);
			log.write(row.toByteArray());
			log.writeBytes("\n");
			index.put(rKey, offset);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized Row getRowForDisplay(String rKey) throws Exception {
		if (!index.containsKey(rKey)) {
			return null;
		}
		Row r = new Row(rKey);
		long pos = index.get(rKey);
		r.put("pos", String.valueOf(pos));
		return r;
	}
	
	public synchronized boolean existRow(String rKey) {
		return index.containsKey(rKey);
	}

	public synchronized Row getRow(String rKey) throws Exception {
		if (!index.containsKey(rKey)) {
			return null;
		}
		long pos = index.get(rKey);
		Row r = null;
		try {
			RandomAccessFile templog = new RandomAccessFile(tableFile, "rws");
			templog.seek(pos);
			FileInputStream fis = new FileInputStream(templog.getFD());
			BufferedInputStream bis = new BufferedInputStream(fis);
			r = Row.readFrom(bis);
			bis.close();
			templog.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return r;
	}
	
	public Row getRowNoLock(String rKey) throws Exception {
		if (!index.containsKey(rKey)) {
			return null;
		}
		long pos = index.get(rKey);
		Row r = null;
		try {
			RandomAccessFile templog = new RandomAccessFile(tableFile, "rws");
			templog.seek(pos);
			FileInputStream fis = new FileInputStream(templog.getFD());
			BufferedInputStream bis = new BufferedInputStream(fis);
			r = Row.readFrom(bis);
			bis.close();
			templog.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return r;
	}

	public boolean persistent() {
		return true;
	}
	
	public int numRows() {
		return index.size();
	}
	
	public synchronized Set<String> getRowKeys() {
		return index.keySet();
	}
	
	public String getKey() {
		return id;
	}
	
	public synchronized boolean rename(String tKey) throws IOException {
		if (tKey.equals(id)) {
			// Do nothing if new key is the same as the old key.
			return true;
		}
		boolean success = false;
		id = tKey;
		File newTable = new File(dir + "/" + id + ".table");
		success = tableFile.renameTo(newTable);
		tableFile = newTable;
		return success;
	}
	public synchronized void delete() throws IOException {
		log.close();
		log = null;
		try {
			Files.delete(tableFile.toPath());
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.tableFile = null;
		
	}
	public synchronized void collectGarbage() throws Exception {
		log.seek(0);
        boolean optimizationNeeded = false;
        Set<String> rows = new HashSet<String>();
        while (true) {
            Row row = Row.readFrom(log);
            if(row == null)
                break;
            if(rows.contains(row.key())) {
                optimizationNeeded = true;
                break;
            }
            rows.add(row.key());
        } 
        if (!optimizationNeeded) {
        	return;
        }
        System.out.println("Garbage collection needed");
		File newTable = new File(dir + "/" + id + ".temp");
		try {
			RandomAccessFile newLog = new RandomAccessFile(newTable, "rws");
			Map<String, Long> newIndex = new ConcurrentHashMap<String, Long>();
			for (String rKey : index.keySet()) {
				long pos = index.get(rKey);
				log.seek(pos);
				Row r = Row.readFrom(log);
				long offset = newLog.length();
			    newLog.write(r.toByteArray());
				newLog.writeBytes("\n");
				newIndex.put(r.key, offset);
			}
			this.log.close();
			newLog.close();
			this.tableFile.delete();
			this.index = newIndex;
			File rename = new File(dir + "/" + id + ".table");
			newTable.renameTo(rename);
			this.tableFile = rename;
			this.log = new RandomAccessFile(this.tableFile, "rws");
		} catch (Exception e) {
			newTable.delete();
		}
		
	}

	public synchronized void putBatch(List<Row> batch, Server server) {
		List<Row> onDisk = new ArrayList<Row>();
		try {
			for (Row temp : batch) {
				if (!index.containsKey(temp.key())) {
					putRow(temp.key(), temp);
					continue;
				} else {
					onDisk.add(temp);
				}
			}
			/*
			Collections.sort(onDisk, new java.util.Comparator<Row>() {
		        @Override
		        public int compare(Row r1, Row r2) {
		        	long i1 = index.get(r1.key());
		        	long i2 = index.get(r2.key());
		        	if (i1 < i2) {
		        		return -1;
		        	} else if (i1 > i2) {
		        		return 1;
		        	} else {
		        		return 0;
		        	}
		        }
			});
			*/
			List<Row> finished = new ArrayList<Row>();
			for (Row temp : onDisk) {
				Row original = getRow(temp.key());
				for (String cKey : temp.columns()) {
					original.put(cKey, temp.get(cKey));
				}
				finished.add(original);
			}
			for (Row f : finished) {
				putRow(f.key(), f);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public List<Row> getParallel(List<Row> toGet, int numThreads, Server server) {
		List<Row> finished = new ArrayList<Row>();
		int numThreadsNeeded = Math.min(toGet.size(), numThreads);
		int each = toGet.size() / numThreads;
		int last = (toGet.size() / numThreads) + (toGet.size() % numThreads);
		LinkedBlockingQueue<Row> got = new LinkedBlockingQueue<Row>();
		for (int i = 0; i < numThreadsNeeded; i++) {
			GetRowTask t = new GetRowTask(this, got);
			if (i != numThreadsNeeded - 1) {
				for (int j = 0; j < each; j++) {
					t.addRows(toGet.get(each * i + j));
				}
			} else {
				for (int j = 0; j < last; j++) {
					t.addRows(toGet.get(each * i + j));
				}
			}
			server.threadPool.dispatch(t);
		}
		// wait for task to finish
		for (int i = 0; i < toGet.size(); i++) {
			try {
				Row r = got.take();
				finished.add(r);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return finished;
	}
	
	public class GetRowTask implements Task {
		PersistentTable t;
		List<Row> toGet;
		LinkedBlockingQueue<Row> got;
		public GetRowTask(Table t, LinkedBlockingQueue<Row> got) {
			toGet = new ArrayList<Row>();
			this.got = got;
			this.t = (PersistentTable) t;
		}
		public void addRows(Row r)  {
			toGet.add(r);
		}
		public void run() {
			try {
				for (Row r : toGet) {
					Row original = t.getRowNoLock(r.key());
					for (String cKey : r.columns()) {
						original.put(cKey, r.get(cKey));
					}
					got.add(original);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}