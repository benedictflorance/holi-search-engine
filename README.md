# Holi Search Engine

Run arguments for testing on local machine:<br>
KVS Master: ``8000``<br>
KVS Worker: ``8001 localhost:8000``<br>
Flame Master: ``9000``<br>
Flame Worker: ``9001 localhost:9000``<br>

Run arguments for EC2:<br>
KVS Master: ``sudo java -cp bin cis5550.kvs.Master 8000``<br>
KVS Worker: ``sudo java -cp bin cis5550.kvs.Worker 8001 worker1 44.198.182.73:8000``<br>
Flame Master: ``sudo java -cp bin cis5550.flame.Master 9000 44.198.182.73:8000``<br>
Flame Worker: ``sudo java -cp bin cis5550.flame.Worker 9001 44.198.182.73:9000``<br>

## Crawler:
Flame submit(run this command on a local machine): ``java -cp bin cis5550.flame.FlameSubmit 44.198.182.73:9000 crawler.jar cis5550.jobs.Crawler https://en.wikipedia.org/``<br>

## Indexer, PageRank, TermFrequency and Idf:
- To compile use 
`javac cis5550/jobs/Indexer.java && jar -cf xxx.jar cis5550/jobs/xxx.class`
- To run use
`java cis5550.flame.FlameSubmit localhost:9000 xxx.jar cis5550.jobs.xxx`


## Ranker:

### Methodology 
- Descending sort of 3 * w_td * w_tq + 0.75 * page_rank, where 
    - w_td uses Euclidean normalized tf_weighting without use of idf
    - w_tq is the product of the query term prequency and the idf raised to 1.5
    - page_rank is the the page rank value resulting out of the iterative page rank algorithm.

### Dependencies:


### Build Instructions 
- To run the ranker, use 
`java -cp lib/\*:src cis5550.ranker.Ranker port kvs_ip:kvs_port`

### Response Format
JSON stringified Java objects, where each object has three fields: 
- **url** - url of the webpage, 
- **title** - title of the webpage (max of 60 characters), 
- **page_head** - snippet of the webpage (max of 300 characters)

### Usage Instructions
- Has a GET request method with path `/search` with 
    - a required query parameter `q` that has the encoded query phrase string 
    - an optional query parameter `page` that defaults to 1 (first page). 
- Each page returns 10 urls. Pages beyond the number of matching urls will return empty strings.

## Frontend:

### Compilation
``javac -cp "lib/*" --source-path src src/cis5550/webserver/TestServer.java``

### Run
``java -cp "../lib/*;" cis5550.webserver.TestServer <frontend-server port> <ranker ip:port>``

### Test
Open browser tab at frontend-server port

### Code Explanation
- TestServer class defined routes:
    - ``/``: this route simply return the home page
    - ``/search?q=&p=``: this route expects a json object like below:

```
class SearchResult {
    String title;
    String url;
    String page_head;

    public SearchResult(String title, String url, String page_head) {
        this.title = title;
        this.url = url;
    }
}


class SearchResultsResponse {
    List<SearchResult> results;
    int page;
    int totalPages;

    public SearchResultsResponse(List<SearchResult> results, int page, int totalPages) {
        this.results = results;
        this.page = page;
        this.totalPages = totalPages;
    }
}
```