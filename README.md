StreamingBetweenness
==================

Introduction:
----------------
This repository hosts the code for a framework that offers streaming betweenness centrality while edges can be added or removed.
Next, I include the standard disclaimer and describe the input options for the multi-machine (batch) version of the software (MapReduce via Hadoop).
Single-machine options can also be derived from this code, as well as only Brandes' algorithm version for speedup comparisons. Feel free to contact me for pointers.

Disclaimer:
---------------
Copyright (c) 2014 Nicolas Kourtellis

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Additional Disclaimer:
-----------------------------
This code was tested on Linux-based Hadoop clusters and works appropriately.
As mentioned above, please use at your own risk. We cannot provide any sort of guarantees that it will work on your platform and specific settings.
Also, we cannot provide any support for you to make it work in case of compilation problems.

Reference:
---------------
If you use this software and its relevant features, please make sure to acknowledge the appropriate studies, by citing:
Nicolas Kourtellis, Gianmarco De Francisci Morales, Francesco Bonchi, "Scalable Online Betweenness Centrality in Evolving Graphs", http://arxiv.org/abs/1401.6981

Input Parameters:
------------------------

Parameters taken at command line:
+++++++++++++++++++++++++++++++++

input path to use on hdfs

output path to use on hdfs

number of Mappers to split the sources: e.g., 1, 10, 100 etc. Rule of thumb: the larger the graph (i.e., number of roots to test), the larger should be this number.

number of Reducers to collect the output

Number of vertices in graph

Number of edges in graph

Graph file (edge list, tab delimited) (full path)

File with edges to be added (tab delimited) (full path). Note: this version handles only edges between existing vertices in the graph.

Number of random edges added

Experiment iteration (in case of multiple experiments)

Use combiner or not (true/false)

Output path for file with stats

Output path for file with final betweenness values


Example of execution call:
++++++++++++++++++++++++++

hadoop jar MapReduce_OptimizedBrandesAdditions_DO_JUNG.jar
-libjars collections-generic-4.01.jar,jung-graph-impl-2.0.1.jar,jung-api-2.0.1.jar
-Dmapred.job.map.memory.mb=4096
-Dmapred.job.reduce.memory.mb=4096
-Dmapred.child.java.opts=-Xmx3500m
-Dmapreduce.task.timeout=60000000
-Dmapreduce.job.queuename=QUEUENAME
input_iterbrandes_additions_nocomb_10k_1 output_iterbrandes_additions_nocomb_10k_1 10 1 10000 55245 10k 10k_randedges 100 1 false times/ betweenness/

Similar input can be used for the version that handles deletions of edges.
