# *ODESSA*: *Odessa* is a Deployment Engine for Scalable Scientific Applications

## Introduction
*ODESSA* is a framework that combines a middleware with the DSL to deploy and execute scientific workflows with scalability. *SWIG*, a simple DSL, allows the user to transform the workflow to a DAG. The middleware can automatically (1) exploit data parallelism opportunities in the workflow, and (2) scale the workflow according to the availability of cloud resources. 

 
## Usage
This repository contains our implementation of *ODESSA*. In particular, the implementation of the master in our middleware architecture is for evaluating the performance of *ODESSA*. It runs each test 26 times (13 times of Random Walk and 13 times of Round Robin).


## Input Files:
### *SWIG* scripts:

 - `workflow_seq.txt` contains the *SWIG* script of the sequential Word Count workflow.
 - `workflow.txt` contains the *SWIG* script of the scalable Word Count workflow.

### Input strings:

 - `input_nMB.txt` is the input file of the Word Count Problem. Each of them include a string that of the size of `n`MB.


### Workers' IPs
The user shall submit cloud resources (e.g., Aliyun ECS VMs) by providing the **private** IP addresses of the master and worker VMs. 

The user should put the IP addresses of all workers in a .txt file (e.g., `worker_ip.txt`) with each address as a string on a line.

The user should also provide the master's IP, `master_ip`, when executing the workers (see the section below).


## Reproduce the Performance Evaluation
 - Upload all the files within /master to the master VM. And upload all the files within /worker to each worker VM.
 - Upload one *SWIG* script to the master.
 - Upload one input string file to one worker VM.
 - Upload the workers' IPs file to the master and each worker. 
 - Run each worker VM with the command:
 `python3 worker.py [master_ip] [worker_ip.txt]`
 - Run the master with the command:
 `python3 master.py [root] [dag.txt] [worker_ip.txt] [input_filename] [input_location]`
where `root` is the root of the workflow DAG (namely `A` in this case), `dag.txt` is the *SWIG* script (`workflow.txt` or `workflow_seq.txt`), `input_filename` is the name of the input file (`input_nMB.txt`), and `input_location` is the IP of the VM which contains the input file.
 
## Output
The output of each 26 tests is generated in the `result.txt` file when the master  program indicates `ALL FINISHED`. The times taken by the Random Walk scheduling algorithm are on the odd number of lines, whereas the times taken by Round Robin are on the even number of lines. 
