package cis5550.ranker;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;

import java.io.IOException;
import com.google.gson.*;
import java.util.*;

import static cis5550.webserver.Server.*;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class Ranker {
    static class SearchResult {
        String title;
        String url;
        String page_head;

        public SearchResult() {
            this.title = null;
            this.url = null;
            this.page_head = null;
        }
    }
    static class SearchResultsResponse {
        List<SearchResult> results;
        int page;
        int totalPages;

        public SearchResultsResponse(List<SearchResult> results, int page, int totalPages) {
            this.results = results;
            this.page = page;
            this.totalPages = totalPages;
        }
    }
    public static class URLWeights{
        String url;
        Integer occurrence;
        Double page_rank;
        Double tf_idf_weight;
        Double keyword_match;
        Map<String, Double> word2tf;
        String title;
        URLWeights(String urlStr)
        {
            url = urlStr;
            occurrence = 1;
            page_rank = 0.0;
            tf_idf_weight = 0.0;
            word2tf = new HashMap<>();
            title = null;
            keyword_match = 0.0;
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
        int getOccurrence(){ return occurrence; };
    }
    public static Set<String> stemWords(String[] words)
    {
        Set<String> wordSet = new HashSet<String>();
        for(String word : wordSet)
        {
            Stemmer s = new Stemmer();
            s.add(word.toCharArray(), word.length());
            s.stem();
            wordSet.add(word);
        }
        return wordSet;
    }
    public static Map<String, URLWeights> findAllMatchingURLs(KVSClient kvs, Set<String> words_stemmed) throws IOException {
        Map<String, URLWeights> urlWeights = new HashMap<>();
        for(String word : words_stemmed)
        {
            byte[] urls_byte = kvs.get("index", word, "url");
            if(urls_byte != null)
            {
                String urls = new String(urls_byte);
                String[] split_urls = urls.split(",");
                for(String url : split_urls)
                {
                    int lastIndex = url.lastIndexOf(':');
                    String urlStr = url.substring(0, lastIndex);
                    Double tf = Double.parseDouble(url.substring(lastIndex+1));
                    String urlHash = Hasher.hash(urlStr);
                    if(urlWeights.containsKey(urlHash))
                    {
                        urlWeights.get(urlHash).occurrence++;
                        urlWeights.get(urlHash).word2tf.put(word, tf);
                    }
                    else
                    {
                        urlWeights.put(urlHash, new URLWeights(urlStr));
                    }
                }
            }
        }
        return urlWeights;
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
        List<String> stopwords = new ArrayList<>(Arrays.asList("a", "about", "above", "after", "again", "against", "ain",
                "all", "am", "an", "and", "any", "are", "aren", "aren't", "as", "at", "be", "because", "been", "before",
                "being", "below", "between", "both", "but", "by", "can", "couldn", "couldn't", "d", "did", "didn",
                "didn't", "do", "does", "doesn", "doesn't", "doing", "don", "don't", "down", "during", "each", "few",
                "for", "from", "further", "had", "hadn", "hadn't", "has", "hasn", "hasn't", "have", "haven", "haven't",
                "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in",
                "into", "is", "isn", "isn't", "it", "it's", "its", "itself", "just", "ll", "m", "ma", "me", "mightn",
                "mightn't", "more", "most", "mustn", "mustn't", "my", "myself", "needn", "needn't", "no", "nor", "not",
                "now", "o", "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over",
                "own", "re", "s", "same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn",
                "shouldn't", "so", "some", "such", "t", "than", "that", "that'll", "the", "their", "theirs", "them",
                "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under",
                "until", "up", "ve", "very", "was", "wasn", "wasn't", "we", "were", "weren", "weren't", "what", "when",
                "where", "which", "while", "who", "whom", "why", "will", "with", "won", "won't", "wouldn", "wouldn't",
                "y", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves", "could",
                "he'd", "he'll", "he's", "here's", "how's", "i'd", "i'll", "i'm", "i've", "let's", "ought", "she'd",
                "she'll", "that's", "there's", "they'd", "they'll", "they're", "they've", "we'd", "we'll", "we're",
                "we've", "what's", "when's", "where's", "who's", "why's", "would"));
        port(port_no);
        get("/search", (req, res) -> {
            KVSClient kvs = new KVSClient(args[1]);
            String search_query = req.queryParams("q");
            String page = req.queryParams("page") == null ? "1" : req.queryParams("page");
            // Convert the words to a set
            String[] words = search_query.replaceAll("[.,:;!?'\"\\(\\)-]", " ")
                    .trim().toLowerCase().split("\\s+");
            if(words.length < 1) // If there are no words, return null
                return "";
            Set<String> words_set = new HashSet<>(Arrays.asList(words));
            // Stem words
            Set<String> stemmedWords = stemWords(words);
            // Remove stopwords
            Set<String> words_sw = new HashSet<>();
            Set<String> removedStopWords = new HashSet<>();
            for (String word : words_set) {
                if (stopwords.contains(word)) {
                    removedStopWords.add(word);
                } else {
                    words_sw.add(word);
                }
            }
            System.out.println("Using just non-stopwords");
            if(words_sw.isEmpty()) // If there are no words, other than stowords, add the stopwords
            {
                System.out.println("No non-stopwords, so using stowords");
                words_sw = removedStopWords;
            }
            // Find all matching urls
            Map<String, URLWeights> urlWeights  = findAllMatchingURLs(kvs, words_sw);
            // If matching urls is lesser than 10, add stemmed words
            Map<String, URLWeights> stemmedUrlWeights = new HashMap<>();
            if(urlWeights.size() < 50) {
                System.out.println("Also using stemmed words");
                stemmedUrlWeights = findAllMatchingURLs(kvs, stemmedWords);
                for(Map.Entry<String, URLWeights> entry : stemmedUrlWeights.entrySet())
                {
                    if(!urlWeights.containsKey(entry.getKey()))
                    {
                        urlWeights.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            if(urlWeights.size() < 50) {
                System.out.println("Using all words + stemmed");
                urlWeights = findAllMatchingURLs(kvs, words_set);
                for(Map.Entry<String, URLWeights> entry : stemmedUrlWeights.entrySet())
                {
                    if(!urlWeights.containsKey(entry.getKey()))
                    {
                        urlWeights.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            if(urlWeights.size() < 1)
                return "";
            List<URLWeights> urlWeightsArray = new ArrayList<>(urlWeights.values());
            Collections.sort(urlWeightsArray, Comparator.comparingInt(URLWeights::getOccurrence).reversed());
            urlWeightsArray = urlWeightsArray.subList(0, min(urlWeightsArray.size(), 250));
            obtainPageRank(kvs, urlWeightsArray);
            Map<String, Integer> wordCount = new HashMap<>();
            for (String word : words) {
                wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
            }
            for(URLWeights urlInfo : urlWeightsArray)
            {
                urlInfo.title = new String(kvs.get("crawl_refined", Hasher.hash(urlInfo.url), "title"));
//                System.out.println(urlInfo.title);
//                System.out.println(search_query);
                // Check overlap between title and query
                if(urlInfo.title.contains(search_query)) {
                    System.out.println("Perfect query match!");
                    urlInfo.keyword_match = 10.0;
                }
                else
                {
                    Set<String> title_set = new HashSet<>(Arrays.asList(urlInfo.title.replaceAll("[.,:;!?'\"\\(\\)-]", " ").trim().toLowerCase().split("\\s+")));
                    Set<String> intersection = new HashSet<>(words_sw); // make a copy of set1
                    intersection.retainAll(title_set); // find intersection of set1 and set2
                    double overlapPercent = ((double) intersection.size() / words_sw.size());
//                    System.out.println(intersection.size() + " " + words_sw.size() + " " + overlapPercent);
                    if(overlapPercent >= 0.6)
                        urlInfo.keyword_match = overlapPercent * 10;
                }
                double weight = 0;
//                System.out.println(urlInfo.url);
                for(Map.Entry<String, Integer> entry : wordCount.entrySet())
                {
                    Double idf;
                    byte[] idf_bytes = kvs.get("w-metric", entry.getKey(), "idf");
                    if(idf_bytes != null)
                        idf = Double.parseDouble(new String(idf_bytes));
                    else
                        idf = 0.0;
                    Double wf;
                    Double num = urlInfo.word2tf.get(entry.getKey());
                    Double den = Double.parseDouble(new String(kvs.get("ntf", urlInfo.url, "ntf")));
                    if(num != null && den != null)
                        wf = num/den;
                    else
                        wf = 0.0;
//                    System.out.println(entry.getKey() + " " + idf + " " + wf + " " + entry.getValue());
                    weight += entry.getValue() * pow(idf, 1.5) * wf;
                }
                urlInfo.set_tf_idf_weight(weight);
//                System.out.println("TFIDF Weight: " + urlInfo.tf_idf_weight);
//                System.out.println("Page Rank: " + urlInfo.page_rank);
            }
            urlWeightsArray.sort(Comparator.comparingDouble(c -> -(3 * c.tf_idf_weight + 0.75 * c.page_rank + c.keyword_match)));
            Map<String, List<URLWeights>> paginatedURLs = new HashMap<>();
            for (int i = 0; i < urlWeightsArray.size(); i += 10) {
                int endIndex = min(i + 10, urlWeightsArray.size());
                List<URLWeights> subList = urlWeightsArray.subList(i, endIndex);
                paginatedURLs.put(String.valueOf(i / 10 + 1), subList);
            }
            if(paginatedURLs.get(page) == null) {
                return "";
            }
            else
            {
                List<SearchResult> urlList = new ArrayList<>();
                for (int i = 0; i < paginatedURLs.get(page).size(); i++) {
                    URLWeights urlWeight = paginatedURLs.get(page).get(i);
                    SearchResult sR = new SearchResult();
                    sR.url = urlWeight.url;
                    sR.title = urlWeight.title;
                    sR.page_head = new String(kvs.get("crawl_refined", Hasher.hash(urlWeight.url), "snippet"));
                    urlList.add(sR);
                }
                SearchResultsResponse sRR = new SearchResultsResponse(urlList, Integer.parseInt(page), paginatedURLs.size());
                return new Gson().toJson(sRR);
            }
        });
    }
}
