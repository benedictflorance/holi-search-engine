package cis5550.kvs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentTable implements Table {
	Map<String, Long> index;
	RandomAccessFile log;
	File tableFile;
	String id;
	String dir;
	
	public PersistentTable(String dir, String tKey) throws FileNotFoundException {
		this.index = new ConcurrentHashMap<String, Long>();
		this.tableFile = new File(dir + "/" + tKey + ".table");
		this.log = new RandomAccessFile(tableFile, "rw");
		this.id = tKey;
		this.dir = dir;
	}
	
	public PersistentTable(String dir, File logFile, String tKey) throws Exception {
		index = new ConcurrentHashMap<String, Long>();
		this.tableFile = logFile;
		this.log = new RandomAccessFile(tableFile, "rw");
		this.id = tKey;
		this.dir = dir;
		recover();
	}
	
	private synchronized void recover() throws Exception {
		try {
			while (log.getFilePointer() != log.length()) {
				long offset = log.getFilePointer();
				Row r = Row.readFrom(log);
				r.values.clear();
				index.put(r.key, offset);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized void putRow(String rKey, Row row) throws Exception {
		try {
			long offset = log.length();
			byte[] rowContent = row.toByteArray();
			log.seek(offset);
			log.write(rowContent);
			byte[] lf = {10};
			log.write(lf);
			row.values.clear();
			index.put(rKey, offset);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Row getRow(String rKey) throws Exception {
		Row r = null;
		try {
			if (!index.containsKey(rKey)) {
				return null;
			}
			long pos = index.get(rKey);
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
	
	public Set<String> getRowKeys() {
		return index.keySet();
	}
	
	public String getKey() {
		return id;
	}
	
	public void setKey(String tKey) {
		id = tKey;
	}
	public synchronized void delete() throws IOException {
		log.close();
		tableFile.delete();
	}
	public synchronized void collectGarbage() throws Exception {
			byte[] lf = {10};
			File newTable = new File(dir + "/" + id + ".temp");
			RandomAccessFile newLog = new RandomAccessFile(newTable, "rw");
			Map<String, Long> newIndex = new ConcurrentHashMap<String, Long>();
			for (String rKey : index.keySet()) {
				long pos = index.get(rKey);
				log.seek(pos);
				Row r = Row.readFrom(log);
				byte[] rowContent = r.toByteArray();
				long offset = newLog.length();
			    newLog.write(rowContent);
				newLog.write(lf);
				newIndex.put(r.key, offset);
			}
			this.log.close();
			this.tableFile.delete();
			this.tableFile = newTable;
			this.log = newLog;
			this.index = newIndex;
			File rename = new File(dir + "/" + id + ".table");
			newTable.renameTo(rename);
	}
}
