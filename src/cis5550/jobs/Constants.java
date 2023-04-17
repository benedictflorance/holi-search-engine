package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Constants {
	public static List<String> blacklist = new ArrayList<String>(Arrays.asList(URLExtractor.buildBadURLsList()));
	public static String CRAWL = "crawl";
	public static String INDEX = "index";
	public static Set<String> ignore = new HashSet<String>(Arrays.asList("the",
																		 "a", 
																		 "an", 
																		 "which", 
																		 "that",
																		 "and", 
																		 "or", 
																		 "for",
																		 "nor",
																		 "but",
																		 "yet",
																		 "so",
																		 "that",
																		 "which",
																		 "who",
																		 "as",
																		 "of"
																		 ));
}
