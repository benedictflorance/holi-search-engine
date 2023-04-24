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
	RandomAccessFile log;
	BufferedOutputStream bos;
	File tableFile;
	String id;
	String dir;
	
	public AppendOnly(String tKey, String dir) throws IOException {
		this.tableFile = new File(dir + "/" + tKey + ".appendOnly");
		this.tableFile.createNewFile();
		this.log = new RandomAccessFile(tableFile, "rws");
		this.bos = new BufferedOutputStream(new FileOutputStream(tableFile));
		this.id = tKey;
		this.dir = dir;
	}

	public AppendOnly(String tKey, String dir, File logFile) throws Exception {
		this.tableFile = logFile;
		this.log = new RandomAccessFile(tableFile, "rws");
		this.bos = new BufferedOutputStream(new FileOutputStream(tableFile));
		this.id = tKey;
		this.dir = dir;
		recover();
	}
	
	public AppendOnly(String tKey, String dir, Map<String, Row> data) throws Exception {
		this(tKey, dir);
		for (String rKey : data.keySet()) {
			putRow(rKey, data.get(rKey));
		}
		
	}
	
	private synchronized void recover() throws Exception {

	}

	public synchronized void putRow(String rKey, Row row) throws Exception {
		try {
			byte[] lf = {10};
			bos.write(row.toByteArray());
			bos.write(lf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized Row getRowForDisplay(String rKey) throws Exception {
		return null;
	}
	
	public synchronized boolean existRow(String rKey) {
		return false;
	}

	public synchronized Row getRow(String rKey) throws Exception {
		return null;
	}
	
	public Row getRowNoLock(String rKey) throws Exception {
		return null;
	}

	public boolean persistent() {
		return true;
	}
	
	public int numRows() {
		return 0;
	}
	
	public synchronized Set<String> getRowKeys() {
		return new HashSet<String>();
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
			while (rows.size() < 2000 && log.getFilePointer() < log.length()) {
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
			System.out.println("Collected 2000 rows in memory");
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(tableFile));
			bis.skip(log.getFilePointer());
			while (bis.available() > 0) {
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