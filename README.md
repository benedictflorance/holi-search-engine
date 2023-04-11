# Holi Search Engine

## Crawler
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

## Indexer:
- To compile the Indexer, use 
`javac -cp src/cis5550/lib/jazzy-0.5.2-rtext-1.4.1-2.jar --source-path src src/cis5550/jobs/Indexer.java`

### Dependencies:
- https://jazzy.sourceforge.net/ (already jar is added in lib/)

## Ranker:

### Methodology 
- Descending sort of 3 * w_td * w_tq + 0.75 * page_rank, where 
    - w_td uses Euclidean normalized tf_weighting without use of idf
    - w_tq is the product of the query term prequency and the idf raised to 1.5
    - page_rank is the the page rank value resulting out of the iterative page rank algorithm.

### Dependencies:
- [com.google.code.gson:gson:2.10.1](https://github.com/google/gson) (already jar is added in lib/)

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

