# PageRank

This repo contains files that implement a basic model of Google's famous PageRank algorithm for assigning scores to webpages in the internet, which is then used to rank pages in the search engine results. 

I have implemented PageRank in three different paradigms:
1. Standard Python (standard_pagerank.ipynb)
2. MapReduce (mrjob_pagerank.py)
3. PySpark (pyspark_pagerank.ipynb) *WIP*

The PageRank implementations read txt files (graph-1.txt, graph-2.txt, wikipedia-example.txt) to execute. In these, the internet is represented using incidence vector representation, where for each node we list its neighbors (the
order of the neighbors does not matter).
