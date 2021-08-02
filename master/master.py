#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 10 19:12:27 2021

@author: verasun
"""



"""
Master: 
    1), waiting machines
    2), constant polling

Graph Expansion:
    1), PR数量

"""
import Parser_1 as Parser
import random
import math
import socket
import threading
import Graph_Expansion_New as GE
import Random_Walk as RW
import time
import sys



"""
Official code starting from here
"""
# The DAG class: 对应documentation里的DAG Progress
class DAG():
    def __init__(self, task_lst):
        self.tasks = self.list_to_dict(task_lst) # a dictionary of tasks: {task_name : Task instance}
    
    def list_to_dict(self, task_lst):
        task_dict = {}
        for t in task_lst:
            task_dict[t.name] = t
        return task_dict
    
    def get_task_func(self, task_name):
        return self.tasks[task_name].func

    def get_task_pred(self, task_name):
        return self.tasks[task_name].preds
    
    def get_task_state(self, task_name):
        return self.tasks[task_name].state
    
    def set_task_state(self, task_name, state):
        self.tasks[task_name].set_state(state)
        
    def set_task_machine(self, task_name, machineID):
        self.tasks[task_name].set_machine_ID(machineID)
    
    def get_ready_tasks(self):
        task_name = []
        for name, t in self.tasks.items():
            flag = 1
            if t.state == "idle":
                for p in t.preds:
                    if self.tasks[p].state != "completed":
                        flag = 0
                if flag == 1:
                    task_name.append(name)

        return task_name
    
    def check_all_completed(self):
        for t in self.tasks:
            if self.tasks[t].state != "completed":
                return False
        return True
    

def read_ip_list(file):
    with open(file, "r") as f:
        ips = f.read().split()
    for i in ips:
        i = i.strip()
    
    d = {}
    for i in ips:
        d[i] = 2
    return ips, d


# one thread: continuous running this process of picking ready tasks and assigning to a VM
def assign(input_filenames, input_locations):
    global vm_index
    while True:

        
        rtasks = D.get_ready_tasks()
        if len(rtasks) == 0:
            continue
        
        ready_task = random.choice(rtasks) #randomly找到一个ready task
        if ready_task == "A":
            print("Spotted a new workflow!")

        preds = D.get_task_pred(ready_task) #找到这个ready task的所有predecessors
        
        if scheduling == "LB":
            print("scheduling: LB")
            # start random walk
            start_node = random.choice(graph_nodes) # randomly在graph上pick一个起始点
            machine = RW.walk(thresh, graph_nodes, graph_edges, start_node) #random walk ends at machine
        else:
            print("scheduling: RR")
            machine = vm_list[vm_index % len(vm_list)]
            RR_lock.acquire()
            vm_index += 1
            print("RR vm_index", vm_index)
            RR_lock.release()
            
            
        print("task {} is scheduled on machine {}".format(ready_task, machine))
        
        RW_lock.acquire()
        RW.remove_edge(machine, graph_edges) # remove an in-edge of machine
        RW_lock.release()

        D_lock.acquire()
        #change state for the running node
        D.set_task_state(ready_task, "running") # task state从idle变成running
        D.set_task_machine(ready_task, machine) # 记录execute这个task的machine ID (IP)
        D_lock.release()
        
        master_D, d = {}, {}
        master_D["type"] = "master"
        d["prefix"] = D.tasks[ready_task].prefix
        d["index"] = D.tasks[ready_task].index
        d["func"] = D.tasks[ready_task].func
        d["num_succ"] = len(D.tasks[ready_task].succs)

        if len(preds) != 0: # if the task has predecessors:
            min_index = 1000000
            all_ips = []
            for p in preds:
                if D.tasks[p].index < min_index:
                    min_index = D.tasks[p].index
                all_ips.append(D.tasks[p].machineID)
            d["preds"] = {D.tasks[p].prefix: all_ips}
            d["start_index"] = min_index
                

        #e.g., 这个task有两个preds (C01 & C02), 分别在IP3和IP4上运行
        #to_machine = {"IP3": ["IP3_C01_1.txt", "IP3_C01_2.txt"], "IP4": ["IP4_C02_1.txt"]}

        else: # if the task is the entry node, we let it know the filenames and the locations of the initial input
            d["preds"] = {"input": input_locations}
            d["start_index"] = 0
        
        master_D["content"] = d
        

        s = socket.socket()
        port = 12345
        s.connect((machine, port))
        whole_data = str(master_D) + "?"
        s.sendall(whole_data.encode())
        print("master send a command of task {} to {}".format(ready_task, machine))
        

        if D.check_all_completed():
            continue
        


        

        
        

def report():
    global curr_complete, result_to_file, all_finish
    
    s = socket.socket()
    host = socket.gethostname()
    s.bind((host, 8899))
    s.listen(5)

    while True:

        c, addr = s.accept()
        if addr[0] not in vm_list:
            c.close()
            continue
        received = eval(c.recv(2048).decode())
        
        print("master receive:", received)
        #{type: complete, task: prefix_index}
        task_name = received["task"]
        dash = task_name.find("_")
        prefix, index = task_name[: dash], int(task_name[dash+1 :])

        D_lock.acquire()
        for t in D.tasks.values():
            if t.prefix == prefix and t.index == index:
                t.state = "completed"
                machine = t.machineID
        D_lock.release()
        

        RW_lock.acquire()
        RW.add_edge(machine, graph_nodes, graph_edges) # graph加一根in-edge
        RW_lock.release()
        
        c.close()
        
        if D.check_all_completed():
            end_time = time.time()
            print(start_time, end_time, end_time - start_time)
            result_to_file.write(str(end_time - start_time)+"\n")
            control_lock.acquire()
            curr_complete = True
            control_lock.release()
            
            d = {"type": "all complete"}
            
            for ip in vm_dict.keys():
                 sp = socket.socket()
                 port = 12345
                 sp.connect((ip, port))
                 whole_data = str(d) + "?"
                 sp.sendall(whole_data.encode())
                
            
            
            
            continue


    
def multiple_test():
    global curr_complete, start_time, D, graph_nodes, graph_edges, scheduling, vm_index, result_to_file
    global all_finish
    sch_index = 0
    while WF_QUEUE:
        if curr_complete == False:
            continue
        else:
            print("in control else")
            time.sleep(3)
            item = WF_QUEUE.pop(0)
            
            D_lock.acquire()
            D = item[0]
            D_lock.release()
            
            RW_lock.acquire()
            graph_nodes, graph_edges = item[1], item[2]
            RW_lock.release()
            
            if sch_index % 2 == 0:
                sch_lock.acquire()
                scheduling = "LB"
                sch_lock.release()
            else:
                sch_lock.acquire()
                scheduling = "RR"
                sch_lock.release()
                RR_lock.acquire()
                vm_index = 0
                RR_lock.release()

            control_lock.acquire()
            curr_complete = False
            control_lock.release()
            start_time = time.time()
            sch_index += 1
            
    while not curr_complete:
        pass
    print("ALL FINISHED!!!")
    result_to_file.close()
        

if __name__ == "__main__":
    
    
   
    """
    
    root = "A"
    #dag_path = "/Users/verasun/Desktop/workflow.txt"
    dag_path = "workflow_seq.txt"
    #vm_dict = {'192.168.2.198': 2, "192.168.2.203": 2, "192.168.2.204": 2, "192.168.2.201": 2,
               #"192.168.2.202": 2, "192.168.2.200": 2}
    #vm_dict = {'172.17.170.77': 2, "172.17.108.231": 2, "172.17.108.230": 2, "172.17.170.80": 2,
               #"172.17.170.81": 2, "172.17.170.78": 2}
    #vm_dict = {'192.168.2.198': 2, "192.168.2.203": 2, "192.168.2.204": 2, "192.168.2.201": 2}

    vm_list, vm_dict = read_ip_list("ip6.txt")
    input_filenames, input_locations = ["input.txt"], ["192.168.2.203"] # the input data partitions from users and their location (IP), 一一对应
    
    """

    root, dag_path, ip_file, input_name, input_loc = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    vm_list, vm_dict = read_ip_list(ip_file)
    input_filenames, input_locations = [input_name], [input_loc] 
    
    
    
    D_lock = threading.Lock()
    RW_lock = threading.Lock()
    control_lock = threading.Lock()
    sch_lock = threading.Lock()
    RR_lock = threading.Lock()
    
    thresh = math.ceil(math.log(len(vm_dict)))
    scheduling = None
    vm_index = 0
    
    
    WF_QUEUE = []
    for i in range(26):
        tasks, edges = Parser.run_parser(dag_path) # run the parser
        new_tasks = GE.begin_expansion(len(vm_dict), root, tasks, edges)[0]
        #for t in new_tasks:
            #t.display()
        D = DAG(new_tasks) # build the DAG
        graph_nodes, graph_edges = RW.generate_graph(vm_dict)
        
        WF_QUEUE.append([D, graph_nodes, graph_edges])
    
    curr_complete = True
    start_time, end_time = -1, -1
    all_finish = False
    
    result_to_file = open("result.txt", "w")
    

    assign_thread = threading.Thread(target = assign, args = (input_filenames, input_locations))
    report_thread = threading.Thread(target = report)
    control_thread = threading.Thread(target = multiple_test)
    control_thread.start()
    time.sleep(5)
    assign_thread.start()
    report_thread.start()

    

    
    
