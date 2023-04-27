package cis5550.jobs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import cis5550.kvs.*;
public class Sort {
	String PATH = "";
	public static void main(String[] args) throws Exception {
		divideAndSort("/Users/seankung/upenn/cis555/holi-search-engine/worker1/pairs.appendOnly");
		for (int i = 0; i < 8; i += 2) {
			String s0 = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort" + i + ".table";
			String s1 = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort" + (i + 1) + ".table";
			String d = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort" + i + "-" + (i + 1) + ".table";
			merge(s0, s1, d);
		}
		for (int i = 0; i < 8; i += 4) {
			String s0 = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort" + i + "-" + (i + 1) + ".table";
			String s1 = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort" + (i + 2) + "-" + (i + 3) + ".table";
			String d = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort" + i + "-" + (i + 1) + "-" + (i + 2) + "-" + (i + 3) + ".table";
			merge(s0, s1, d);
		}
		String s0 = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort0-1-2-3.table";
		String s1 = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort4-5-6-7.table";
		String d = "/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort0-1-2-3-4-5-6-7.table";
		merge(s0, s1, d);
		
		collapse("/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort0-1-2-3-4-5-6-7.table", "/Users/seankung/upenn/cis555/holi-search-engine/worker1/collapsed.table");
		produceIndex("/Users/seankung/upenn/cis555/holi-search-engine/worker1/collapsed.table", "/Users/seankung/upenn/cis555/holi-search-engine/worker1/index.table");
	}
	
	public static void divideAndSort (String inFile) throws IOException, Exception {
		System.out.println("Calculating total rows.");
		byte[] lf = {10};
		File in = new File (inFile);
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(in));
		int num = 0;
		while (bis.available() > 0) {
			Row r = Row.readFrom(bis);
			if (r == null) {
				break;
			}
			num++;
		}
		System.out.println("Number of rows: " + num);
		System.out.println("Divide the file into 8 pieces and sort them.");
		bis.close();
		
		int each = num / 8;
		int last = num / 8 + num % 8;
		bis = new BufferedInputStream(new FileInputStream(in));
		
		for (int i = 0; i < 8; i++) {
			List<Row> ls = new ArrayList<Row>();
			int quota;
			if (i < 7) {
				quota = each;
			} else {
				quota = last;
			}
			while (bis.available() > 0 && ls.size() <= quota) {
				Row r = Row.readFrom(bis);
				if (r == null) {
					break;
				}
				ls.add(r);
			}
			Collections.sort(ls, new Comparator<Row>() {
		        @Override
		        public int compare(Row r1, Row r2) {
		        	return r1.key().compareTo(r2.key());
		        }
			});
			File out = new File ("/Users/seankung/upenn/cis555/holi-search-engine/worker1/sort" + i + ".table");
			out.createNewFile();
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
			for (Row sort : ls) {
				bos.write(sort.toByteArray());
				bos.write(lf);
			}
			bos.flush();
			bos.close();
		}
		bis.close();
		System.out.println("Sorting completed");
	}
	
	public static void merge(String file1, String file2, String out) throws Exception {
		byte[] lf = {10};
		System.out.println("Merging " + file1 +  " and " + file2);
		File s0 = new File (file1);
		File s1 = new File (file2);
		File merge = new File (out);
		BufferedInputStream i0 = new BufferedInputStream(new FileInputStream(s0));
		BufferedInputStream i1 = new BufferedInputStream(new FileInputStream(s1));
		BufferedOutputStream m = new BufferedOutputStream(new FileOutputStream(merge));
		Row r0 = Row.readFrom(i0);
		Row r1 = Row.readFrom(i1);
		while (i0.available() > 0 && i1.available() > 0) {
			if (r0 == null || r1 == null) {
				break;
			}
			if (r0.key().compareTo(r1.key()) < 0) {
				m.write(r0.toByteArray());
				m.write(lf);
				r0 = Row.readFrom(i0);
			} else {
				m.write(r1.toByteArray());
				m.write(lf);
				r1 = Row.readFrom(i1);
			}
		}
		if (r0 == null && r1 != null) {
			m.write(r1.toByteArray());
			m.write(lf);
			while (i1.available() > 0) {
				r1 = Row.readFrom(i1);
				m.write(r1.toByteArray());
				m.write(lf);
			}
		} else if (r0 != null && r1 == null) {
			m.write(r0.toByteArray());
			m.write(lf);
			while (i0.available() > 0) {
				r0 = Row.readFrom(i0);
				m.write(r0.toByteArray());
				m.write(lf);
			}
		} else if (r0 != null && r1 != null) {
			if (r0.key().compareTo(r1.key()) < 0) {
				m.write(r0.toByteArray());
				m.write(lf);
			} else {
				m.write(r1.toByteArray());
				m.write(lf);
			}
		}
		m.close();
		i0.close();
		i1.close();
	}
	public static void collapse(String fileIn, String fileOut) throws IOException, Exception {
		System.out.println("Collapsing the sorted file.");
		File in = new File (fileIn);
		File out = new File (fileOut);
		byte[] lf = {10};
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(in));
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
		Row curr = Row.readFrom(bis);
		while (bis.available() > 0) {
			Row r = Row.readFrom(bis);
			if (r == null) {
				bos.write(curr.toByteArray());
				bos.write(lf);
				curr = r;
				break;
			}
			if (r.key().equals(curr.key())) {
				for (String c : r.columns()) {
					curr.put(c, r.get(c));
				}
			} else {
				bos.write(curr.toByteArray());
				bos.write(lf);
				curr = r;
			}
		}
		if (curr != null) {
			bos.write(curr.toByteArray());
			bos.write(lf);
		}
		bis.close();
		bos.flush();
		bos.close();
	}
	
	public static void produceIndex(String inFile, String outFile) throws IOException, Exception {
		System.out.println("Producing index.");
		byte[] lf = {10};
		File in = new File (inFile);
		File out = new File (outFile);
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(in));
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
		while (bis.available() > 0) {
			Row r = Row.readFrom(bis);
			if (r == null) {
				break;
			}
			StringBuilder sb = new StringBuilder();
			for (String c : r.columns()) {
				sb.append(r.get(c));
				sb.append(",");
			}
			sb.setLength(sb.length() - 1);
			Row p = new Row(r.key());
			p.put("url", sb.toString());
			bos.write(p.toByteArray());
			bos.write(lf);
		}
		bis.close();
		bos.close();
	}
	
	public static List<File> divideAndSort (File in) throws IOException, Exception {
		System.out.println("Calculating total rows.");
		byte[] lf = {10};
		List<File> ret = new ArrayList<File>();
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(in));
		int num = 0;
		while (bis.available() > 0) {
			Row r = Row.readFrom(bis);
			if (r == null) {
				break;
			}
			num++;
		}
		System.out.println("Number of rows: " + num);
		System.out.println("Divide the file into 8 pieces and sort them.");
		bis.close();
		
		int each = num / 8;
		int last = num / 8 + num % 8;
		bis = new BufferedInputStream(new FileInputStream(in));
		
		for (int i = 0; i < 8; i++) {
			List<Row> ls = new ArrayList<Row>();
			int quota;
			if (i < 7) {
				quota = each;
			} else {
				quota = last;
			}
			while (bis.available() > 0 && ls.size() <= quota) {
				Row r = Row.readFrom(bis);
				if (r == null) {
					break;
				}
				ls.add(r);
			}
			Collections.sort(ls, new Comparator<Row>() {
		        @Override
		        public int compare(Row r1, Row r2) {
		        	return r1.key().compareTo(r2.key());
		        }
			});
			File out = new File ("sort-" + i + ".table");
			out.createNewFile();
			ret.add(out);
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
			for (Row sort : ls) {
				bos.write(sort.toByteArray());
				bos.write(lf);
			}
			bos.flush();
			bos.close();
		}
		bis.close();
		System.out.println("Sorting completed");
		return ret;
	}
	
	public static void merge(File s0, File s1, File merge) throws Exception {
		byte[] lf = {10};
		System.out.println("Merging " + s0.getName() +  " and " + s1.getName());
		BufferedInputStream i0 = new BufferedInputStream(new FileInputStream(s0));
		BufferedInputStream i1 = new BufferedInputStream(new FileInputStream(s1));
		BufferedOutputStream m = new BufferedOutputStream(new FileOutputStream(merge));
		Row r0 = Row.readFrom(i0);
		Row r1 = Row.readFrom(i1);
		while (i0.available() > 0 && i1.available() > 0) {
			if (r0 == null || r1 == null) {
				break;
			}
			if (r0.key().compareTo(r1.key()) < 0) {
				m.write(r0.toByteArray());
				m.write(lf);
				r0 = Row.readFrom(i0);
			} else {
				m.write(r1.toByteArray());
				m.write(lf);
				r1 = Row.readFrom(i1);
			}
		}
		if (r0 == null && r1 != null) {
			m.write(r1.toByteArray());
			m.write(lf);
			while (i1.available() > 0) {
				r1 = Row.readFrom(i1);
				m.write(r1.toByteArray());
				m.write(lf);
			}
		} else if (r0 != null && r1 == null) {
			m.write(r0.toByteArray());
			m.write(lf);
			while (i0.available() > 0) {
				r0 = Row.readFrom(i0);
				m.write(r0.toByteArray());
				m.write(lf);
			}
		} else if (r0 != null && r1 != null) {
			if (r0.key().compareTo(r1.key()) < 0) {
				m.write(r0.toByteArray());
				m.write(lf);
			} else {
				m.write(r1.toByteArray());
				m.write(lf);
			}
		}
		m.close();
		i0.close();
		i1.close();
	}
	
	public static void collapse(File in, File out) throws IOException, Exception {
		System.out.println("Collapsing the sorted file.");
		byte[] lf = {10};
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(in));
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
		Row curr = Row.readFrom(bis);
		while (bis.available() > 0) {
			Row r = Row.readFrom(bis);
			if (r == null) {
				bos.write(curr.toByteArray());
				bos.write(lf);
				curr = r;
				break;
			}
			if (r.key().equals(curr.key())) {
				for (String c : r.columns()) {
					curr.put(c, r.get(c));
				}
			} else {
				bos.write(curr.toByteArray());
				bos.write(lf);
				curr = r;
			}
		}
		if (curr != null) {
			bos.write(curr.toByteArray());
			bos.write(lf);
		}
		bis.close();
		bos.flush();
		bos.close();
	}
	
}