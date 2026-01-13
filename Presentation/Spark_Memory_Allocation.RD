Optimized Spark memory and node allocation is a balancing act that involves careful calculation of 
resources and workload characteristics. The goal is to maximize parallelism while avoiding excessive overhead 
and Out-of-Memory (OOM) errors. 
General Principles for Resource Allocation
Reserve System Resources: Always leave 1 core and 1-2 GB of RAM per node for the operating system and Hadoop daemons.
Optimal Cores per Executor: Aim for 3 to 5 cores per executor to balance parallelism and HDFS throughput/garbage collection efficiency. Too many cores per executor can lead to task contention and long GC pauses, while too few increase the number of JVMs and overhead.
Account for Memory Overhead: Spark executors require off-heap (non-JVM) memory for internal processes, which is not included in --executor-memory. The cluster manager (YARN, Kubernetes) allocates this as a total container size.
Formula: Total Container Memory = --executor-memory + spark.executor.memoryOverhead
spark.executor.memoryOverhead is max(384MB, 7-10% of --executor-memory).
Driver Memory: The driver needs enough memory to store the job execution plan, manage the DAG, and collect results (e.g., via collect() actions). A typical starting point is 4-8 GB, but complex jobs may require significantly more. 
Step-by-Step Calculation Example (Static Allocation)
Consider a cluster with 6 nodes, each having 16 cores and 64 GB RAM. 
Calculate Available Resources Per Node:
Cores: 16 total - 1 reserved for OS/daemons = 15 available cores.
Memory: 64 GB total - 1 GB reserved for OS/daemons = 63 GB available memory.
Determine Executors Per Node:
Use the optimal 5 cores per executor rule: 15 available cores / 5 cores per executor = 3 executors per node.
Calculate Total Executors:
3 executors/node * 6 nodes = 18 total executors.
Subtract 1 executor for the Application Master (in YARN cluster mode): 17 executors.
Calculate Executor Memory (On-Heap):
Memory per node is 63 GB. Distributed across 3 executors/node: 63 GB / 3 = 21 GB per executor (container limit).
Calculate memory overhead (~7% of 21 GB is ~1.47 GB, round up to 2-3 GB for buffer).
Executor memory: 21 GB (container) - 3 GB (overhead) = 18 GB.
Calculate Driver Memory:
The driver typically runs on the master node and requires resources for coordination and result collection.
A safe default starting point is 8 GB to 10 GB, but this should be monitored and adjusted based on job complexity. 
