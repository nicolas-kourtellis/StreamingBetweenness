// Copyright (c) 2014 Nicolas Kourtellis
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// Additional Disclaimer:
// 
// This code was tested on Linux-based Hadoop clusters and works appropriately.
// As mentioned above, please use at your own risk. We cannot provide any sort of
// guarantees that it will work on your platform and specific settings.
// Also, we cannot provide any support for you to make it work in case of compilation
// problems.
//
// If you use this software and its relevant features, please make sure to acknowledge
// the appropriate studies, by citing:
//
// Nicolas Kourtellis, Gianmarco De Francisci Morales, Francesco Bonchi,
// Scalable Online Betweenness Centrality in Evolving Graphs, http://arxiv.org/abs/1401.6981

package map_reduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.collections15.Buffer;
import org.apache.commons.collections15.buffer.UnboundedFifoBuffer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.jung.graph.UndirectedSparseGraph;

public class MapReduce_OptimizedBrandesAdditions_DO_JUNG extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new MapReduce_OptimizedBrandesAdditions_DO_JUNG(), args);
		System.exit(exitCode);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage:\n");
			System.exit(1);
		}
		
		//	    Job job = new Job(super.getConf());
	    
//		READ IN ALL COMMAND LINE ARGUMENTS
//		EXAMPLE: 
		// hadoop jar MapReduce_OptimizedBrandesAdditions_DO_JUNG.jar
		// -libjars collections-generic-4.01.jar,jung-graph-impl-2.0.1.jar,jung-api-2.0.1.jar
		// -Dmapred.job.map.memory.mb=4096
		// -Dmapred.job.reduce.memory.mb=4096
		// -Dmapred.child.java.opts=-Xmx3500m
		// -Dmapreduce.task.timeout=60000000
		// -Dmapreduce.job.queuename=QUEUENAME
		// input_iterbrandes_additions_nocomb_10k_1 output_iterbrandes_additions_nocomb_10k_1
		// 10 1 10000 55245 10k 10k_randedges 100 1 false times/ betweenness/
		
		int m = -1;

		// input path to use on hdfs
		Path inputPath = new Path(args[++m]);
		
		// output path to use on hdfs
		Path outputPath = new Path(args[++m]);
		
		// number of Mappers to split the sources: e.g., 1, 10, 100 etc.
		// rule of thumb: the larger the graph (i.e., number of roots to test), the larger should be this number.
		int numOfMaps = Integer.parseInt(args[++m]);
		
		// number of Reducers to collect the output
		int numOfReduce = Integer.parseInt(args[++m]);
		
		// Number of vertices in graph
		int N = Integer.parseInt(args[++m]);
		
		// Number of edges in graph
		int M = Integer.parseInt(args[++m]);
		
		// Graph file (edge list, tab delimited) (full path)
		String graph = args[++m];
		
		// File with edges to be added (tab delimited) (full path)
		// Note: this version handles only edges between existing vertices in the graph.
		String random_edges = args[++m];
		
		// Number of random edges added
		int re = Integer.parseInt(args[++m]);
		
		// Experiment iteration (in case of multiple experiments)
		int iter = Integer.parseInt(args[++m]);
		
		// Use combiner or not (true/false)
		Boolean comb = Boolean.valueOf(args[++m]);
		
		// Output path for file with stats
		String statsoutputpath = args[++m];
		
		// Output path for file with final betweenness values
		String betoutputpath = args[++m];
		

//		BEGIN INITIALIZATION
		
		JobConf conf = new JobConf(getConf(), MapReduce_OptimizedBrandesAdditions_DO_JUNG.class);
		FileSystem fs = FileSystem.get(conf);

		String setup = "_additions_edges"+re+"_maps"+numOfMaps+"_comb"+comb;
		conf.setJobName("OptimizedBrandesAdditionsDOJung_"+graph+setup+"_"+iter);
		conf.set("HDFS_GRAPH", graph+setup);
		conf.set("HDFS_Random_Edges", random_edges+setup);
		conf.set("output", outputPath.getName());
		conf.set("setup", setup);
		
//		CREATE INPUT FILES FOR MAPPERS
		
		int numOfTasksperMap = (int)Math.ceil(N/numOfMaps);
		//generate an input file for each map task
		for(int i=0; i < numOfMaps-1; i++) {
			Path file = new Path(inputPath, "part-r-"+i);
			IntWritable start = new IntWritable(i * numOfTasksperMap);
			IntWritable end = new IntWritable((i * numOfTasksperMap) + numOfTasksperMap - 1);
			
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file, IntWritable.class, IntWritable.class, CompressionType.NONE);
			try {
				writer.append(start, end);
			} finally {
				writer.close();
			}
			System.out.println("Wrote input for Map #"+i+": "+start+" - "+end);
		}
		
		// last mapper takes what is left
		Path file = new Path(inputPath, "part-r-"+(numOfMaps-1));
		IntWritable start = new IntWritable((numOfMaps-1) * numOfTasksperMap);
		IntWritable end = new IntWritable(N-1);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file, IntWritable.class, IntWritable.class, CompressionType.NONE);
		try {
			writer.append(start, end);
		} finally {
			writer.close();
		}
		System.out.println("Wrote input for Map #"+(numOfMaps-1)+": "+start+" - "+end);

//		COPY FILES TO MAPPERS
		System.out.println("Copying graph to cache");
		String LOCAL_GRAPH = graph;
		Path hdfsPath = new Path(graph+setup);

		// upload the file to hdfs. Overwrite any existing copy.
		fs.copyFromLocalFile(false, true, new Path(LOCAL_GRAPH), hdfsPath);
		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
		
		System.out.println("Copying random edges to cache");
		String LOCAL_Random_Edges = random_edges;
		hdfsPath = new Path(random_edges+setup);
		
		// upload the file to hdfs. Overwrite any existing copy.
		fs.copyFromLocalFile(false, true, new Path(LOCAL_Random_Edges), hdfsPath);
		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(IterBrandesMapper.class);
		conf.setNumMapTasks(numOfMaps);

		if(comb)
			conf.setCombinerClass(IterBrandesReducer.class);
		
		conf.setReducerClass(IterBrandesReducer.class);
		conf.setNumReduceTasks(numOfReduce);

		// turn off speculative execution, because DFS doesn't handle multiple writers to the same file.
		conf.setSpeculativeExecution(false);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		// conf.set("mapred.job.name", "APS-" + outputPath.getName());
		conf.setNumTasksToExecutePerJvm(-1); // JVM reuse
		
		System.out.println("Starting the execution...! Pray!! \n");
		long time1 = System.nanoTime();
		RunningJob rj = JobClient.runJob(conf);
		long time2 = System.nanoTime();

//		READ OUTPUT FILES
		
		System.out.println("\nFinished and now reading/writing Betweenness Output...\n");

		// Assuming 1 reducer.
		Path inFile = new Path(outputPath, "part-00000");
		IntWritable id = new IntWritable();
		DoubleWritable betweenness = new DoubleWritable();
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, conf);
		
		FileWriter fw = new FileWriter(new File(betoutputpath+graph+setup+"_betweenness_"+iter));
		try {
			int i=0;
			for(; i< (N + M + re); i++){
				reader.next(id, betweenness);
				fw.write(id+"\t"+betweenness+"\n");
				fw.flush();
			}
		} finally {
			reader.close();
			fw.close();
		}
		
		System.out.println("\nWriting times Output...\n");
		
		fw = new FileWriter(new File(statsoutputpath+graph+setup+"_times_"+iter));

		fw.write("Total-time:\t"+(time2-time1)+"\n");
		fw.write("total-map\t"+rj.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").getCounter("SLOTS_MILLIS_MAPS")+"\n");
		fw.write("total-reduce\t"+rj.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").getCounter("SLOTS_MILLIS_REDUCES")+"\n");
		fw.write("total-cpu-mr\t"+rj.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").getCounter("CPU_MILLISECONDS")+"\n");
		fw.write("total-gc-mr\t"+rj.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").getCounter("GC_TIME_MILLIS")+"\n");
		fw.write("total-phy-mem-mr\t"+rj.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").getCounter("PHYSICAL_MEMORY_BYTES")+"\n");
		fw.write("total-vir-mem-mr\t"+rj.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").getCounter("VIRTUAL_MEMORY_BYTES")+"\n");
		fw.write("brandes\t"+rj.getCounters().getGroup("TimeForBrandes").getCounter("exectime_initial_brandes")+"\n");
		fw.write("reduce\t"+rj.getCounters().getGroup("TimeForReduce").getCounter("reduceafteralledges")+"\n");
		fw.flush();
		
		try {
			Iterator<Counters.Counter> counters = rj.getCounters().getGroup("TimeForRandomEdges").iterator();
			while(counters.hasNext()){
				Counter cc = counters.next();
					fw.write(cc.getName()+"\t"+cc.getCounter()+"\n");
					fw.flush();
			}
		} finally {
			fw.close();
		}
		
		return 0;
	}

	public static class IterBrandesMapper extends MapReduceBase implements Mapper<IntWritable, IntWritable, IntWritable, DoubleWritable> {

		JobConf conf;
		
		static UndirectedSparseGraph<Integer, String> G = new UndirectedSparseGraph<Integer, String>();
		static HashMap<String, Double> EBC = new HashMap<String, Double>();
		static HashMap<Integer, Double> NBC = new HashMap<Integer, Double>();
		static ArrayList<String> edges = new ArrayList<String>();
		static ArrayList<String> mods;
		static String to_be_written;
		static int V = 0;
		
	    public static int[] distance;
	    public static int[] numSPs;
	    public static double[] dependency;
	    
	    public static FileOutputStream outStream;
	    public static FileInputStream inStream;
	    public static FileChannel fcin;
	    public static FileChannel fcout;
		public static long pos_read =0;
		public static long pos_write =0;
		
		// Configure our setup
		@SuppressWarnings("deprecation")
		@Override
		public void configure(JobConf conf) {
			
			this.conf = conf;
			
			try {
				String graphCacheName = conf.get("HDFS_GRAPH");
				String random_edgesCacheName = conf.get("HDFS_Random_Edges");

				Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
				if (null != cacheFiles && cacheFiles.length > 0) {
					for (Path cachePath : cacheFiles) {
						
						// Read the graph file: <n1 n2>
						if (cachePath.getName().equals(graphCacheName)) {
							readgraph(cachePath.getName());
						}
						// Read the random edges file: <n1 n2>
						if (cachePath.getName().equals(random_edgesCacheName)){
							mods=random_edges(cachePath);
						}
					}
				}
			} catch (IOException ioe) {
				System.err.println("IOException reading from distributed cache");
				System.err.println(ioe.toString());
			}
		}

		public static void readgraph(String graph){
			String fileData = null;
			if ((new File(graph)).exists()) {
				try {
					BufferedReader reader = new BufferedReader( new FileReader(graph));
					fileData=reader.readLine();
					while(fileData!=null){
						int v0 = Integer.parseInt(fileData.split("\t")[0]);
						int v1 = Integer.parseInt(fileData.split("\t")[1]);

						if (!G.containsVertex(v0)) {
							G.addVertex((Integer)v0);
						}
						if (!G.containsVertex((Integer)v1)) {
							G.addVertex(v1);
						}
						String edge = "";
						if(v0<v1)
							edge = v0+"_"+v1;
						else edge = v1+"_"+v0;
						G.addEdge(edge, v0, v1);
						fileData=reader.readLine();
					}
					reader.close();
					System.out.println("Edges: "+G.getEdgeCount()+" Vertices: "+G.getVertexCount());
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("Error while reading the file "+graph);
				}
			}
		}
		
		public ArrayList<String> random_edges(Path edges){
			ArrayList<String> mods = new ArrayList<String>();
			String fileData = null;
			if ((new File(edges.toString())).exists()) {
				try {
					BufferedReader reader = new BufferedReader( new FileReader(edges.toString()));
					fileData=reader.readLine();
					while(fileData!=null){
						mods.add(fileData);
						fileData=reader.readLine();
					}
					reader.close();
					System.out.println("Edges: "+mods.size());
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("Error while reading the file "+edges);
				}
			}
			return mods;
		}

		@Override
		public void map(IntWritable start, IntWritable end, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {

			to_be_written = "tw_"+start.get()+"_"+end.get()+conf.get("setup");
			V=G.getVertexCount();
			
	        outStream = new FileOutputStream(new File(to_be_written));
	        fcout  = outStream.getChannel();
	        inStream = new FileInputStream(to_be_written);
	        fcin  = inStream.getChannel();

	        System.out.println("start: "+start+" end:"+end);
			try {
				
//				FIRST BRANDES TO INITIALIZE DATA STRUCTURES IN DISK FILE				
				long time1=System.nanoTime();
				computeBetweenness(start.get(), end.get());
				long time2=System.nanoTime();
				
				reporter.incrCounter("TimeForBrandes", "exectime_initial_brandes", (time2-time1));
				reporter.incrCounter("NumberOfEdges", "edges", 1);
				
				// System.out.println("Initial\tBrandes\t"+(time2-time1));
				
				int ed=0;
				for(String e:mods){
					int A = Integer.parseInt(e.split("\t")[0]);
					int B = Integer.parseInt(e.split("\t")[1]);

					String edge = "";
					if(A<B) edge = A+"_"+B;
					else edge = B+"_"+A;
					if(G.containsEdge(edge)){
						System.out.println("I already have this edge!");
					} else {
						if ((G.containsVertex(A))&&(G.containsVertex((Integer)B))) {
							G.addEdge(edge, A, B);
							System.out.println((++ed)+"\t"+A+"\t"+B+"\tIterative Algorithm recalculating on the modified graph");
							
//							REPEAT STREAMING BETWEENNESS FOR EACH NEW EDGE ADDED
							time1=System.nanoTime();
							computeStreamingBetweenness(A, B, start.get(), end.get());
							time2=System.nanoTime();
							reporter.incrCounter("TimeForRandomEdges", edge, (time2-time1));
						}
					}
					reporter.incrCounter("NumberOfEdges", "edges", 1);
				}
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
//			EMITTING OUTPUT of RESULTS for the reducer(s)
			int i=0;
			IntWritable intw = new IntWritable(i);
			DoubleWritable dbw = new DoubleWritable(NBC.get(i)/2);
			for (; i<V; i++){
				intw.set(i);
				dbw.set(NBC.get(i)/2);
				output.collect(intw, dbw);
			}
			for (String e : G.getEdges()){
				intw.set(i);
				dbw.set(EBC.get(e)/2);
				output.collect(intw, dbw);
				i++;
			}
		}
		
		public static void computeBetweenness(int start, int end) throws IOException, InterruptedException {
	        for (Integer vertex : G.getVertices()) {
	        	NBC.put(vertex, 0.0);
	        }
	        for (String e : G.getEdges()) {
	        	EBC.put(e, 0.0);
	        }
	        
			long time1=System.nanoTime();
			long time2;
			
			int n=0;
	        for(int s=start; s<=end;s++){
	        	n++;

	        	if(n%1000==0){
	        		time2=System.nanoTime();
	        		System.out.println(n+"\t"+(time2-time1));
	        		time1=System.nanoTime();
	        	}

	        	numSPs = new int[V];
	        	distance = new int[V];
	        	dependency = new double[V]; 

	        	//Initialization of arrays
	            for(int i=0; i<V;i++){
	            	numSPs[i]=0;
	            	distance[i]=-1;
	            	dependency[i]=0;
	            }
	        	numSPs[s]=1;
	        	distance[s]=0;

	            Stack<Integer> stack = new Stack<Integer>();
	            Buffer<Integer> queue = new UnboundedFifoBuffer<Integer>();
	            queue.add(s);

	            // System.out.println("\nBFS Traversal\n");
	            while (!queue.isEmpty()) {
	                Integer v = queue.remove();
	                stack.push(v);
	                // System.out.println("\nPopping user v: "+v);

	        		for(Integer w : G.getNeighbors(v)) {
	        			// System.out.println("Checking neighbor w: "+w);
	                    if (distance[w] < 0) {
	                    	queue.add(w);
	                        distance[w] = distance[v] + 1;
	                    }
	                    if (distance[w] == distance[v] + 1) {
	                        numSPs[w] += numSPs[v];
	                    }
	                }
	            }
	            
	            // System.out.println("\nDependency backwards traversal\n");
	            while (!stack.isEmpty()) {
	                Integer w = stack.pop();
	                // System.out.println("\nPopping user w: "+w);

	                for (Integer v : G.getNeighbors(w)) {
	                	//	System.out.println("Checking user v: "+v);

	                	if(distance[v]<distance[w]){
	                		double partialDependency = (numSPs[v]*1.0 /numSPs[w]);
	                        partialDependency *= (1.0 + dependency[w]);
	                        dependency[v] +=  partialDependency;

	                        String currentEdge = "";
	                        if(v<w) currentEdge = v+"_"+w;
	                        else currentEdge = w+"_"+v;
	                        EBC.put(currentEdge, EBC.get(currentEdge)+partialDependency);
	                	}
	                }
	                if (w != s) {
	                	NBC.put(w, NBC.get(w)+dependency[w]);
	                }
	            }
	            
	            BRwriteout(s);
	        }
		}
		
		public static void BRwriteout(int s) throws IOException{
			ByteBuffer d1 = ByteBuffer.allocate(V*4);
			ByteBuffer d2 = ByteBuffer.allocate(V*4);
			ByteBuffer d3 = ByteBuffer.allocate(V*8);

	        for (int j=0;j<V; j++) {
	        	d1.putInt(distance[j]);
	        	d2.putInt(numSPs[j]);
	        	d3.putDouble(dependency[j]);
	        }
	        d1.flip();
	        fcout.write(d1);
	        d2.flip();
	        fcout.write(d2);
	        d3.flip();
	        fcout.write(d3);
		}
		
		public static void STREAMseek(long ind1, long ind2, int u1, int u2) throws IOException{
			
			fcin.position(ind1);
			ByteBuffer du = ByteBuffer.allocate(4);
	    	fcin.read(du);
	    	du.flip();
	    	distance[u1]=du.getInt();

			fcin.position(ind2);
			ByteBuffer du2 = ByteBuffer.allocate(4);
	    	fcin.read(du2);
	    	du2.flip();
	    	distance[u2]=du2.getInt();
	    	// System.out.println("seek "+distance[u1]+" "+distance[u2]);
		}
		
		public static void STREAMreadin() throws IOException{
			
			fcin.position(pos_read);
			
			ByteBuffer d1 = ByteBuffer.allocate(V*4);
			fcin.read(d1);
			d1.flip();
			IntBuffer ib1 = d1.asIntBuffer();
	    	for(int i=0; i<V; i++){
	    		distance[i]=ib1.get(i);
	    	}

			ByteBuffer d2 = ByteBuffer.allocate(V*4);
			fcin.read(d2);
			d2.flip();
			IntBuffer ib2 = d2.asIntBuffer();
	    	for(int i=0; i<V; i++){
	    		numSPs[i]=ib2.get(i);
	    	}
	    	
			ByteBuffer d3 = ByteBuffer.allocate(V*8);
			fcin.read(d3);
			d3.flip();
			DoubleBuffer ib3 = d3.asDoubleBuffer();
	    	for(int i=0; i<V; i++){
	    		dependency[i]=ib3.get(i);
	    	}
	    	// System.out.println("read");
		}
		
		public static void STREAMwriteout(int[] numSPsnew, int[] distancenew, double[] dependencynew, short[] t) throws IOException{
			fcout.position(pos_write);
	        
			ByteBuffer d1 = ByteBuffer.allocate(V*4);
			ByteBuffer d2 = ByteBuffer.allocate(V*4);
			ByteBuffer d3 = ByteBuffer.allocate(V*8);

	        for (int j=0;j<V; j++) {
	        	d1.putInt(distancenew[j]);
	        	d2.putInt(numSPsnew[j]);
	    		if(t[j]!=0)
	    			d3.putDouble(dependencynew[j]);
	        	else
	        		d3.putDouble(dependency[j]);
	        }
	        d1.flip();
	        fcout.write(d1);
	        d2.flip();
	        fcout.write(d2);
	        d3.flip();
	        fcout.write(d3);
	        // System.out.println("write");
		}
		
		@SuppressWarnings("unchecked")
		public static void computeStreamingBetweenness(int u1, int u2, int start, int end) throws IOException, InterruptedException {
			
			int uhigh =-1;
			int ulow =-1;
			
			String edgeadded = "";

	        if(u1<u2){
	        	EBC.put(u1+"_"+u2, 0.0);
	        	edgeadded = u1+"_"+u2;
	        }
	        else {
	        	EBC.put(u2+"_"+u1, 0.0);
	        	edgeadded = u2+"_"+u1;
	        }

	        int n = 0;
	        long time1=0,time2=0;
	        pos_read=0;
	        pos_write=0;
	        for(int s=start; s<=end;s++){
	        	if(start==-1)
	        		start = s;
	        	
	        	if(n%1000==0){
	        		time2=System.nanoTime();
	        		System.out.println("Covered "+n+" sources in "+"\t"+(time2-time1));
	        		time1=System.nanoTime();
	        	}

	        	long ind1 = (long)(s-start)*16*V+u1*4;
	        	long ind2 = (long)(s-start)*16*V+u2*4;
	        	
	        	STREAMseek(ind1,ind2, u1, u2);
	        	
	        	if(distance[u1]<=distance[u2]){
	        		uhigh=u1;
	        		ulow=u2;
	        	} else if(distance[u1]>distance[u2]){
	        		uhigh=u2;
	        		ulow=u1;
	        	}

//	        	NODES CONNECTED AT SAME LEVEL
//				NO CHANGE SO SKIP
	        	// System.out.println(distance[u1]+" "+distance[u2]);
	        	if(distance[u1]==distance[u2]) {
	            	pos_read += 16*V;
	            	pos_write=pos_read;
	        	}
	        	
//				NODES CONNECTED AT ONE LEVEL APART
//				TYPICAL CASE AND NOT SO EXPENSIVE	        	
	        	else if(distance[ulow]-distance[uhigh]==1.0){

	        		STREAMreadin();
	            	pos_read += 16*V;
	            	
	    			UnboundedFifoBuffer<Integer>[] Levelqueues = new UnboundedFifoBuffer[V+1];
	    			UnboundedFifoBuffer<Integer> BFSqueue = new UnboundedFifoBuffer<Integer>();
	                
	                for (int i=0; i<=V; i++){
	                	Levelqueues[i] = new UnboundedFifoBuffer<Integer>();
	                }

	            	int[] numSPsnew = new int[V];
	            	int[] distancenew = new int[V];
	            	double[] dependencynew = new double[V]; 
	            	short[] t = new short[V];
	            	
	        		for (int i=0;i<V;i++) {
	        			numSPsnew[i]=numSPs[i];
	        			distancenew[i]=distance[i];
	        			dependencynew[i]=0;
	        			t[i]=0;
	                }

	                BFSqueue.add(ulow);
	                Levelqueues[distancenew[ulow]].add(ulow);
	                t[ulow] = -1;
	                numSPsnew[ulow] += numSPs[uhigh];

	                // System.out.println("\nBFS Traversal\n");
	                while (!BFSqueue.isEmpty()) {
	                    int v = BFSqueue.remove();

	                    for(int w : G.getNeighbors(v)) {
	                    	if (distancenew[w] == distancenew[v] + 1) {
	                    		if (t[w]==0) {
	                    			BFSqueue.add(w);
	                    			Levelqueues[distancenew[w]].add(w);
	                    			t[w] = -1;
	                    		}
	                    		numSPsnew[w] += numSPsnew[v] - numSPs[v];
	                    	}
	                    }
	                }

	                // System.out.println("\nDependency backwards traversal\n");
	                int level = V;
	                while (level>0){
	                	// System.out.println("\n\nLevel: "+level);
	                	while (!Levelqueues[level].isEmpty()) {
	                		int w = Levelqueues[level].remove();
	                		// System.out.println("\nPopping user w: "+w);
	                		
	                        for (int v : G.getNeighbors(w)) {
	                        	
	                        	// System.out.println("\nChecking user v: "+v);
	                        	if(distancenew[v]<distancenew[w]){
	                            	if (t[v]==0) {
	                            		Levelqueues[level-1].add(v);
	                            		t[v] = 1;
	                            		dependencynew[v] = dependency[v];
	                            	}
	                            	//Update dependency
	                        		double partialDependency = (numSPsnew[v]*1.0 /numSPsnew[w]);
	                                partialDependency *= (1.0 + dependencynew[w]);
	                                dependencynew[v] +=  partialDependency;

	                                //Update EBC
	                                String currentEdge = "";
	                                if(v<w) currentEdge = v+"_"+w;
	                                else currentEdge = w+"_"+v;
	                                EBC.put(currentEdge, EBC.get(currentEdge)+partialDependency);
	                              
	                            	double partialDependency_ = (numSPs[v]*1.0 /numSPs[w]);
	                            	partialDependency_ *= (1.0 + dependency[w]);
	                                if ( (t[v]==1) && ((v!=uhigh)||(w!=ulow))  ) {
	                                    dependencynew[v] -=  partialDependency_;
	                                }
	                                if(!currentEdge.equals(edgeadded))
	                                	EBC.put(currentEdge, EBC.get(currentEdge)-partialDependency_);
	                        	}
	                        }
	                        if (w != s) {
	                        	NBC.put(w, NBC.get(w)+dependencynew[w]-dependency[w]);
	                        }
	                	}
	                	level--;
	                }

	                STREAMwriteout(numSPsnew, distancenew, dependencynew, t);
	                pos_write=pos_read;
	        	}

	        	
//				NODES CONNECTED AT 2 OR MORE LEVELS APART.
//				LESS COMMON CASE BUT WITH MORE COMPLEXITY
	        	else if(distance[ulow]-distance[uhigh]>1.0){
	        		
	        		STREAMreadin();
	            	pos_read += 16*V;

	    			UnboundedFifoBuffer<Integer>[] Levelqueues = new UnboundedFifoBuffer[V+1];
	    			UnboundedFifoBuffer<Integer> BFSqueue = new UnboundedFifoBuffer<Integer>();
	                
	                for (int i=0; i<=V; i++){
	                	Levelqueues[i] = new UnboundedFifoBuffer<Integer>();
	                }
	                
	            	int[] numSPsnew = new int[V];
	            	int[] distancenew = new int[V];
	            	double[] dependencynew = new double[V]; 
	            	short[] t = new short[V];
	            	ArrayList<String> decorator_e = new ArrayList<String>();
	            	
	        		for (int i=0;i<V;i++) {
	        			numSPsnew[i]=numSPs[i];
	        			distancenew[i]=distance[i];
	        			dependencynew[i]=0;
	        			t[i]=0;
	                }
	                
	                BFSqueue.add(ulow);
	                distancenew[ulow] = distance[uhigh]+1;
	                Levelqueues[distancenew[ulow]].add(ulow);

	                //BFS traversal starting at ulow
	                while (!BFSqueue.isEmpty()) {
	                    int v = BFSqueue.remove();
	                    // System.out.println("\nPopping user v: "+v);

	                    numSPsnew[v]=0;
	            		t[v] = -1;

	            		// System.out.println("\nProceeding with shortest paths");
	                    for(int w : G.getNeighbors(v)) {

	                    	// System.out.println("Checking neighbor w: "+w);

	                		//The neighbor is a valid predecessor and its shortest paths should be inherited.
	                    	if (distancenew[w]+1 == distancenew[v]) {
	                    		numSPsnew[v] += numSPsnew[w];
	                		}

	                		// The neighbor is a successor so should be placed in the BFS queue and Levels Queue.
	            			else if (distancenew[w] > distancenew[v]) {
	                    		if (t[w] == 0) {
	                    			// System.out.println("I was previously untouched: "+w);
	                    			distancenew[w] = distancenew[v] + 1;
	                    			Levelqueues[distancenew[w]].add(w);
	                    			BFSqueue.add(w);
	                    			t[w] = -1;
	                    		}
	            			}
	                		// The neighbor is at same level so should be placed in the BFS queue and Levels Queue and EBC should be removed.
	            			else if(distancenew[w] == distancenew[v]){
	                       		if(distance[w]!=distance[v]){
	                       			if (t[w] == 0) {
	                        			t[w] = -1;
	                        			Levelqueues[distancenew[w]].add(w);
	                        			BFSqueue.add(w);
	                        		}
	                       		}
	            			}
	                    }
	                }
	                
	                // System.out.println("\nDependency backwards traversal\n");
	                int level = V;
	                while (level>0){
	                	// System.out.println("\nLevel: "+level);
	                	while (!Levelqueues[level].isEmpty()) {
	                		int w = Levelqueues[level].remove();
	                		
	                		// System.out.println("\nPopping user w: "+w);
	                		
	                        for (int v : G.getNeighbors(w)) {
	                        	
	                        	if(distancenew[v]<distancenew[w]){
	                        		if(t[v] == 0) {
	                        			Levelqueues[level-1].add(v);
	                        			t[v] = 1;
	                        			dependencynew[v] = dependency[v];
	                        		}
	                              
	                        		double partialDependency = (numSPsnew[v]*1.0 /numSPsnew[w]);
	                        		partialDependency *= (1.0 + dependencynew[w]);
	                        		dependencynew[v] +=  partialDependency;

	                        		String currentEdge = "";
	                        		if(v<w) currentEdge = v+"_"+w;
	                        		else currentEdge = w+"_"+v;
	                        		EBC.put(currentEdge, EBC.get(currentEdge)+partialDependency);

	                        		double partialDependency_ = 0;
	                        		if ( (t[v]==1) && ((v!=uhigh)||(w!=ulow))  ) {
	                        			partialDependency_ = (numSPs[v]*1.0 /numSPs[w]);
	                        			partialDependency_ *= (1.0 + dependency[w]);                            
	                        			dependencynew[v] -=  partialDependency_;
	                        		}

	                        		partialDependency_ = 0;
	                        		if(distance[v]==distance[w]) {
	                        			partialDependency_ = 0;
	                        		} else {
	                        			if(distance[w]>distance[v]){
	                        				partialDependency_ = (numSPs[v]*1.0 /numSPs[w]);
	                             			partialDependency_ *= (1.0 + dependency[w]);                            
	                             		} else if(distance[w]<distance[v]){
	                             			partialDependency_ = (numSPs[w]*1.0 /numSPs[v]);
	                             			partialDependency_ *= (1.0 + dependency[v]);                            
	                             		}
	                        		}
	                              
	                        		if(!currentEdge.equals(edgeadded))
	                        			EBC.put(currentEdge, EBC.get(currentEdge)-partialDependency_);                              
	                        	}
	                        	
	                        	if((distancenew[v]==distancenew[w]) && (distance[w]!=distance[v])){
	                    			String currentEdge = "";
	                    			if(v<w) currentEdge = v+"_"+w;
	                    			else currentEdge = w+"_"+v;

	                    			if(!decorator_e.contains(currentEdge)){
	                        			decorator_e.add(currentEdge);
	                        			
	                        			double partialDependency_ = 0;
	                        			if(distance[w]>distance[v]){
	                                    	partialDependency_ = (numSPs[v]*1.0 /numSPs[w]);
	                                    	partialDependency_ *= (1.0 + dependency[w]);
	                               		} else if(distance[w]<distance[v]){
	                                    	partialDependency_ = (numSPs[w]*1.0 /numSPs[v]);
	                                    	partialDependency_ *= (1.0 + dependency[v]);                            
	                               		}

	                                    EBC.put(currentEdge, EBC.get(currentEdge)-partialDependency_);
	                    			}
	                        	}
	                        	
	                        }

	                        if (w != s) {
	                        	NBC.put(w, NBC.get(w)+dependencynew[w]-dependency[w]);
	                        }
	                	}
	                	level--;
	                }
	                
	                STREAMwriteout(numSPsnew, distancenew, dependencynew, t);
	                pos_write=pos_read;
	        	}
	        	n++;
	        }
		}

	}
		
	public static class IterBrandesReducer extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		
		@Override
		public void reduce(IntWritable key, Iterator<DoubleWritable> values,  OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
			// Read output file and sum across all ranges.
			double sum=0;
			long time1=System.nanoTime();
			while(values.hasNext()){
				sum+=values.next().get();
			}
			long time2=System.nanoTime();
			output.collect(key, new DoubleWritable(sum));
			reporter.incrCounter("TimeForReduce", "reduceafteralledges", (time2-time1));
		}			
	}
}
