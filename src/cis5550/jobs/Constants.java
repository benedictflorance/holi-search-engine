package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Constants {
	public static List<String> blacklist = new ArrayList<String>(Arrays.asList(Crawler.buildBadURLsList()));
	public static String CRAWL = "crawl";
	public static String INDEX = "index";
}
