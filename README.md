# PageRank

This repo contains files that implement a basic model of Google's famous PageRank algorithm for assigning scores to webpages in the internet to determine the page's importance relative to the other pages. 

![PageRank Algorithm](https://github.com/charleyjzhao/pagerank/blob/master/pagerank_algorithm.png)


I have implemented PageRank in three different paradigms:
1. Standard Python (standard_pagerank.ipynb)
2. MapReduce (mrjob_pagerank.py)
3. PySpark (pyspark_pagerank.ipynb) *Work in progress*

The PageRank implementations read txt files (graph-1.txt, graph-2.txt, wikipedia-example.txt) to execute. In these, the internet is represented using incidence vector representation, where for each node we list its neighbors (the
order of the neighbors does not matter).
