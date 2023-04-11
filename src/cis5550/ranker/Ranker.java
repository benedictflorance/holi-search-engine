package cis5550.ranker;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;

import java.io.IOException;
import com.google.gson.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cis5550.webserver.Server.*;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class Ranker {
    public static class URLWeights{
        String url;
        Double page_rank;
        Double tf_idf_weight;
        URLWeights(String urlStr)
        {
            url = urlStr;
            page_rank = null;
            tf_idf_weight = null;
        }
        void set_page_rank(double rank)
        {
            page_rank = rank;
        }
        void set_tf_idf_weight(double weight)
        {
            tf_idf_weight = weight;
        }
        String get_url()
        {
            return url;
        }
    }
    public static String randomAlpha(int len) {
        Random x = new Random();
        String theVal = "";
        for (int i=0; i<len; i++)
            theVal = theVal + "abcdefghijklmnopqrstuvwxyz".charAt(x.nextInt(26));
        return theVal;
    }
    public static Set<String> stemWords(String[] words)
    {
        Set<String> wordSet = new HashSet<String>(Arrays.asList(words));
        for(String word : wordSet)
        {
            Stemmer s = new Stemmer();
            s.add(word.toCharArray(), word.length());
            s.stem();
            wordSet.add(word);
        }
        return wordSet;
    }
    public static Set<String> findAllMatchingURLs(KVSClient kvs, Set<String> words_stemmed) throws IOException {
        Set<String> matching_urls = new HashSet<>();
        for(String word : words_stemmed)
        {
            String urls = new String(kvs.get("index", word, "tf"));
            if(urls != null)
            {
                String[] split_urls = urls.split(",");
                for(String url : split_urls)
                {
                    int lastIndex = url.lastIndexOf(':');
                    matching_urls.add(url.substring(0, lastIndex));
                }
            }
        }
        return matching_urls;
    }
    public static void obtainPageRank(KVSClient kvs, List<URLWeights> urlWeights) throws IOException {
        for(URLWeights urlInfo : urlWeights)
        {
            if(kvs.get("pageranks", urlInfo.url, "rank") != null)
                urlInfo.set_page_rank(Double.parseDouble(new String(kvs.get("pageranks", urlInfo.url, "rank"))));
            else
                urlInfo.set_page_rank(0.0);
        }
    }
    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("Syntax: port kvs_ip:kvs_port");
            System.exit(1);
        }
        int port_no = Integer.parseInt(args[0]);
        port(port_no);
        get("/search", (req, res) -> {
            KVSClient kvs = new KVSClient(args[1]);
            String search_query = req.queryParams("q");
            String page = req.queryParams("page") == null ? "1" : req.queryParams("page");
            String[] words = search_query.trim().split("\\s+");
            Set<String> words_stemmed = stemWords(words);
            Set<String> urls = findAllMatchingURLs(kvs, words_stemmed);
            List<URLWeights> urlWeights = urls.stream()
                    .map(url -> new URLWeights(url))
                    .collect(Collectors.toList());
            obtainPageRank(kvs, urlWeights);
            Map<String, Integer> wordCount = new HashMap<>();
            for (String word : words) {
                wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
            }
            for(URLWeights urlInfo : urlWeights)
            {
                double weight = 0;
//                System.out.println(urlInfo.url);
                for(Map.Entry<String, Integer> entry : wordCount.entrySet())
                {
                    Double idf;
                    if(kvs.get("w-metric", entry.getKey(), "idf") != null)
                        idf = Double.parseDouble(new String(kvs.get("w-metric", entry.getKey(), "idf")));
                    else
                        idf = 0.0;
                    Double wf;
                    if(kvs.get("wd-metric", Hasher.hash(urlInfo.url) + ":" + entry.getKey(), "normalized-tf") != null)
                        wf = Double.parseDouble(new String(kvs.get("wd-metric", Hasher.hash(urlInfo.url) + ":" + entry.getKey(), "normalized-tf")));
                    else
                        wf = 0.0;
//                    System.out.println(entry.getKey() + " " + idf + " " + wf + " " + entry.getValue());
                    weight += entry.getValue() * pow(idf, 1.5) * wf;
                }
                urlInfo.set_tf_idf_weight(weight);
//                System.out.println("TFIDF Weight: " + urlInfo.tf_idf_weight);
//                System.out.println("Page Rank: " + urlInfo.page_rank);
            }
            urlWeights.sort(Comparator.comparingDouble(c -> -(3 * c.tf_idf_weight + 0.75 * c.page_rank)));
            Map<String, List<URLWeights>> paginatedURLs = new HashMap<>();
            for (int i = 0; i < urlWeights.size(); i += 10) {
                int endIndex = min(i + 10, urlWeights.size());
                List<URLWeights> subList = urlWeights.subList(i, endIndex);
                paginatedURLs.put(String.valueOf(i / 10 + 1), subList);
            }
            if(paginatedURLs.get(page) == null) {
                return "";
            }
            else
            {
                List<Map<String, Object>> urlList = new ArrayList<>();
                for (int i = 0; i < paginatedURLs.get(page).size(); i++) {
                    URLWeights urlWeight = paginatedURLs.get(page).get(i);
//                    System.out.println("URL: " + urlWeight.url);
//                    System.out.println("TFIDF Weight: " + urlWeight.tf_idf_weight);
//                    System.out.println("Page Rank: " + urlWeight.page_rank);
                    String html_page = new String(kvs.get("crawl", Hasher.hash(urlWeight.url), "page"));
                    Map<String, Object> item = new HashMap<>();
                    item.put("url", urlWeight.url);

                    // Extract the title
                    Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
                    Matcher titleMatcher = titlePattern.matcher(html_page);
                    if (titleMatcher.find()) {
                        String title = titleMatcher.group(1);
                        title = title.substring(0, min(60, title.length()));
                        item.put("title", title);
                    }
                    // Extract the body
                    Pattern bodyPattern = Pattern.compile("<body>(.*?)</body>", Pattern.DOTALL);
                    Matcher bodyMatcher = bodyPattern.matcher(html_page);
                    if (bodyMatcher.find()) {
                        String body = bodyMatcher.group(1);
                        body = body.replaceAll("\\<.*?\\>", " ");
                        body = body.replaceAll("[.,:;!?'\"()\\-\\p{Cntrl}]", " ");
                        body = body.substring(0, min(body.length(), 300));
                        item.put("page_head", body);
                    }
                    String default_text = html_page.replaceAll("\\<.*?\\>", " ")
                            .replaceAll("[.,:;!?'\"()\\-\\p{Cntrl}]", " ");
                    String default_body = default_text.substring(0, min(default_text.length(), 300));
                    String default_title = default_text.substring(0, min(default_text.length(), 60));
                    if(item.get("title") == null)
                        item.put("title", default_title);
                    if(item.get("page_head") == null)
                        item.put("page_head", default_body);
                    urlList.add(item);
                }
                return new Gson().toJson(urlList);
            }
        });
    }
}
