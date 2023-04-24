package cis5550.kvs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.webserver.Server;
import cis5550.webserver.ThreadPool.Task;

public class AppendOnly implements Table {
	Map<String, Long> index;
	RandomAccessFile log;
	BufferedOutputStream bos;
	File tableFile;
	String id;
	String dir;
	
	public AppendOnly(String tKey, String dir) throws IOException {
		this.index = new ConcurrentHashMap<String, Long>();
		this.tableFile = new File(dir + "/" + tKey + ".appendOnly");
		this.tableFile.createNewFile();
		this.log = new RandomAccessFile(tableFile, "rws");
		this.bos = new BufferedOutputStream(new FileOutputStream(tableFile));
		this.id = tKey;
		this.dir = dir;
	}

	public AppendOnly(String tKey, String dir, File logFile) throws Exception {
		index = new ConcurrentHashMap<String, Long>();
		this.tableFile = logFile;
		this.log = new RandomAccessFile(tableFile, "rws");
		this.bos = new BufferedOutputStream(new FileOutputStream(tableFile));
		this.id = tKey;
		this.dir = dir;
		recover();
	}
	
	public AppendOnly(String tKey, String dir, Map<String, Row> data) throws Exception {
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
			byte[] lf = {10};
			bos.write(row.toByteArray());
			bos.write(lf);
			long offset = tableFile.length();
			index.put(rKey, (long) offset);
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
		// AppendOnly does not perform garbage collection.
	}

	public synchronized void putBatch(List<Row> batch, Server server) {
		try {
			for (Row temp : batch) {
				putRow(temp.key(), temp);
			}
			this.bos.flush();
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
	
	public synchronized File collapse() throws Exception {
		File out = new File(dir + "/" + id + ".table");
		out.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
		byte[] lf = {10};
		RandomAccessFile log = new RandomAccessFile (tableFile, "r");
		System.out.println("Collapsing.");
		Set<String> done = new HashSet<String>();
		while (log.getFilePointer() < log.length()) {
			float progress =  ((float) log.getFilePointer() / (float) log.length()) * 100.f ;
			System.out.println("Progress: " + progress + "%");
			Map<String, Row> rows = new HashMap<String, Row>();
			while (rows.size() < 1000 && log.getFilePointer() < log.length()) {
				Row r = Row.readFrom(log);
				if (r == null) {
					break;
				}
				if (done.contains(r.key())) {
					continue;
				}
				if (rows.containsKey(r.key())) {
					Row existing = rows.get(r.key());
					for (String c : r.columns()) {
						existing.put(c, r.get(c));
					}
				} else {
					rows.put(r.key(), r);
					done.add(r.key());
				}
			}
			RandomAccessFile temp = new RandomAccessFile (tableFile, "rw");
			temp.seek(log.getFilePointer());
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(temp.getFD()));
			while (temp.getFilePointer() != temp.length()) {
				Row p = Row.readFrom(bis);
				if (p == null) {
					break;
				}
				if (rows.containsKey(p.key())) {
					Row existing = rows.get(p.key());
					for (String c : p.columns()) {
						existing.put(c, p.get(c));
					}
				}
			}
			bis.close();
			temp.close();
			for (Entry<String, Row> e : rows.entrySet()) {
				bos.write(e.getValue().toByteArray());
				bos.write(lf);
			}
		}
		log.close();
		bos.flush();
		bos.close();
		return out;
	}
}