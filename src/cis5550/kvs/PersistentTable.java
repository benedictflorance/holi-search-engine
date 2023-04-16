package cis5550.kvs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

	public synchronized Row getRow(String rKey) throws Exception {
		if (!index.containsKey(rKey)) {
			return null;
		}
		long pos = index.get(rKey);
		Row r = null;
		try {
			log.seek(pos);
			r = Row.readFrom(log);
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
}