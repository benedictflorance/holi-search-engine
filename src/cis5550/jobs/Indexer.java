package cis5550.jobs;
import cis5550.flame.*;

public class Indexer {
	public static void run(FlameContext context, String[] args) throws Exception {
		FlameRDD crawlTable = context.fromTable("crawl",  row -> {
			return row.get("url") + "," + row.get("page");
		});
		FlamePairRDD invertedIndex = crawlTable.mapToPair(str -> {
			int comma = str.indexOf(",");
			return new FlamePair(str.substring(0, comma), str.substring(comma + 1));
		});
		invertedIndex = invertedIndex.flatMapToPair(pair -> {
			String url = pair._1();
			String content = pair._2();
			StringBuilder sb = new StringBuilder();
			boolean insideTag = false;
			java.util.Set<String> words = new java.util.HashSet<String>();
			for (int i = 0; i < content.length(); i++) {
				char c = content.charAt(i);
				if (insideTag) {
					if (c == '>') {
						insideTag = false;
					}
					continue;
				}
				if (c == '<') {
					insideTag = true;
					continue;
				}
				if (isDelim(c)) {
					if (sb.length() > 0) {
						words.add(sb.toString());
						sb.setLength(0);
					}
				} else {
					sb.append(Character.toLowerCase(c));
				}
			}
			java.util.List<FlamePair> ret = new java.util.LinkedList<FlamePair>();
			for (String w : words) {
				ret.add(new FlamePair(w, url));
			}
			return ret;
		});
		invertedIndex = invertedIndex.foldByKey("", (a, b) -> {
			if (a.length() == 0) {
				a = b;
			} else {
				a += "," + b;
			}
			return a;
		});
		
		invertedIndex.saveAsTable("index");
		context.output("OK");
	}
	
	public static boolean isDelim(char c) {
		return (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9');
	}
}
