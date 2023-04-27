package cis5550.kvs;

import static cis5550.kvs.Row.readFrom;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import cis5550.jobs.Trie;

public class IndexFilter {
	
	 public static void main(String args[]) {
		 try {
			 Trie trie = new Trie();
			 trie.buildTrie("/Users/namitashukla/Desktop/holi-search-engine/src/cis5550/jobs/words_alpha.txt");
			 RandomAccessFile indexFilter = new RandomAccessFile("/Users/namitashukla/Desktop/indexfilter.table", "rw");
			 RandomAccessFile file = new RandomAccessFile("/Users/namitashukla/Desktop/index.table", "rw");
	         while(true){
	             Row row = readFrom(file);
	             if(row == null)
	                 break;
	             String rowkey = row.key();
	             if(!trie.containsWord(rowkey)) {
	            	 continue;
	             }
	             if(containsDigitsAndAlphabets(rowkey)) {
	            	 continue;
	             }
	             if(containsLargeDigits(rowkey)) {
	            	 continue;
	             }
	             indexFilter.write(row.toByteArray());
	             indexFilter.writeBytes("\n");
	         }
		 }
		 catch(Exception e) {
			 e.printStackTrace();
		 }
	 }
	 
	 public static boolean containsDigitsAndAlphabets(String str) {
	        boolean containsDigits = str.matches(".*\\d+.*");
	        boolean containsAlphabets = str.matches(".*[a-zA-Z]+.*");
	        return containsDigits && containsAlphabets;
	 }
	 
	 public static boolean containsLargeDigits(String str) {
	        boolean containsDigits = str.matches(".*\\d+.*");
	        return containsDigits && (str.length()>4);
	 }
	 
	 public static void test(String args[]) {
		 //true
		 System.out.println(containsDigitsAndAlphabets("asegdde-12344"));
		 //true
		 System.out.println(containsDigitsAndAlphabets("a1234t5"));
		 //true
		 System.out.println(containsLargeDigits("12345666666"));
		 //false
		 System.out.println(containsLargeDigits("1234"));
		 //false
		 System.out.println(containsDigitsAndAlphabets("abcd"));
	 }
}
