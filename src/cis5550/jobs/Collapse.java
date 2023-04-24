package cis5550.jobs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import cis5550.kvs.Row;

public class Collapse {
	public static void main(String[] args) throws Exception {
		File in = new File ("/Users/seankung/upenn/cis555/holi-search-engine/worker1/append.table");
		File out = new File ("/Users/seankung/upenn/cis555/holi-search-engine/worker1/finish.table");
		out.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
		byte[] lf = {10};
		RandomAccessFile log = new RandomAccessFile (in, "r");
		System.out.println(log.length());
		Set<String> done = new HashSet<String>();
		while (log.getFilePointer() < log.length()) {
			System.out.println(log.getFilePointer());
			Map<String, Row> rows = new HashMap<String, Row>();
			for (int i = 0; i < 100000 && log.getFilePointer() < log.length(); i++) {
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
			RandomAccessFile temp = new RandomAccessFile (in, "rw");
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
		
	}
}
