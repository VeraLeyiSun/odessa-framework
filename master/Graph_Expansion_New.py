#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 17 13:36:16 2021

@author: verasun
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 10:00:38 2021

@author: verasun
"""

import Parser_1 as Parser
import copy
import math

def list_to_dict(task_lst):
    task_dict = {}
    for t in task_lst:
        task_dict[t.name] = t
    return task_dict

def determine_leaf_node(task_dict):
    leaves = []
    for k, v in task_dict.items():
        if len(v.preds) == 0:
            leaves.append(k)
    return leaves

def find_depth(task_dict, root):
    new_tasks = copy.deepcopy(task_dict)
    depth_dict = {}
    height = 0
    while len(new_tasks) != 0:
        leaves = determine_leaf_node(new_tasks)
        for l in leaves:
            depth_dict[l] = height
            for p in new_tasks[l].succs:
                new_tasks[p].preds.remove(l)
            del new_tasks[l]
        height += 1
        
    return depth_dict



def find_diff_sister(tasks, depth_dict, node_name):
    diff_sis = [0, 0] # [# of diff single sisters, # of diff parallel sisters]
    self_depth = depth_dict[node_name]
    
    for k, v in depth_dict.items():
        if v == self_depth and k != node_name: # by default, the funcs that sisters run are all different
            if tasks[k].is_parallel == True:
                diff_sis[1] += 1
            else: 
                diff_sis[0] += 1
                
    return diff_sis

def check_more_pred(edges, node_name):
    count = 0
    for e in edges:
        if e.out_node == node_name:
            count += 1
    return count

# int, int, a dict of {name: Task()}, a list of Edge()
def graph_expansion(vm_num, entry, tasks, edges):

    after_expand = {} # {task name: # of replicas}
    depth_dict = find_depth(tasks, entry)
    
    new_edges = []
    new_tasks = []
    
    for e in edges:
        in_node = e.in_node
        out_node = e.out_node
        
        exist_entry, exist_out = 0, 0
        
        for t in new_tasks: # avoid duplicate adding A to new_tasks
            if t.name == in_node:
                exist_entry = 1
            elif t.name == out_node:
                exist_out = 1
        
        if in_node == entry: # entry node
                    
            if tasks[in_node].is_parallel == False and tasks[in_node].is_reduce == False: # entry is NULL:
                after_expand[in_node] = 1
                if not exist_entry:
                    newt = copy.deepcopy(tasks[in_node])
                    newt.index = 0
                    new_tasks.append(newt)
            elif tasks[in_node].is_reduce == True:
                print("Error: Entry node cannot perform a reduce job")
                return
            else:
                q = pow(2, math.ceil(math.log(vm_num * 0.7)/math.log(2)));
                after_expand[in_node] = q
                if not exist_entry:
                    for i in range(q):
                        newt = copy.deepcopy(tasks[in_node])
                        newt.name += str(i)
                        newt.index = i
                        new_tasks.append(newt)
        
                
        "general case: only handle the out_node"
        if tasks[out_node].is_parallel == False and tasks[out_node].is_reduce == False: # NULL node
            if tasks[in_node].is_parallel == True and tasks[in_node].is_reduce == False: # error: p --> NULL
                print("Error: P->NULL cannot be an edge")
                return
            elif tasks[in_node].is_reduce == True: # pred is reduce / parallel reduce
                new_edges.append(Parser.Edge(in_node, out_node))
            else: # pred is NULL
                new_edges.append(Parser.Edge(in_node, out_node))
            after_expand[out_node] = 1
            
            if not exist_out:
                newt = copy.deepcopy(tasks[out_node])
                newt.index = 0
                new_tasks.append(newt)
        
        elif tasks[out_node].is_parallel == True and tasks[out_node].is_reduce == False: # parallel
            sis_info = find_diff_sister(tasks, depth_dict, out_node)
            q = (vm_num - sis_info[0]) * 0.7 / (sis_info[1] + 1)
            
            single_quota = pow(2, math.ceil(math.log(q)/math.log(2)));
            if single_quota > (vm_num - sis_info[0]) * 0.7 :
                single_quota = int(single_quota / 2)
            
            if (tasks[in_node].is_parallel == False and tasks[in_node].is_reduce == False) or (tasks[in_node].is_parallel == False):
                # pred is NULL, reduce, parallel reduce
                for i in range(single_quota):
                    new_edges.append(Parser.Edge(in_node, out_node + str(i)))
                    newt = copy.deepcopy(tasks[out_node])
                    newt.name += str(i)
                    newt.index = i
                    new_tasks.append(newt)
                after_expand[out_node] = single_quota

            elif tasks[in_node].is_parallel == True and tasks[in_node].is_reduce == False: # pred is parallel 
                pred_quota = after_expand[in_node]
                for i in range(pred_quota):
                    new_edges.append(Parser.Edge(in_node + str(i), out_node + str(i)))
                    newt = copy.deepcopy(tasks[out_node])
                    newt.name += str(i)
                    newt.index = i
                    new_tasks.append(newt)
                after_expand[out_node] = pred_quota

            
        elif tasks[out_node].is_parallel == False and tasks[out_node].is_reduce == True: # reduce
            if tasks[in_node].is_parallel == True and tasks[in_node].is_reduce == False: # pred is parallel
                pred_quota = after_expand[in_node]
                for i in range(pred_quota):
                    new_edges.append(Parser.Edge(in_node + str(i), out_node))
            else: # pred is NULL / reduce / PR
                new_edges.append(Parser.Edge(in_node, out_node))
            after_expand[out_node] = 1
            if not exist_out:
                new_t = copy.deepcopy(tasks[out_node])
                new_t.index = 0
                new_tasks.append(newt)
        
        
        else: # parallel reduce
            sis_info = find_diff_sister(tasks, depth_dict, out_node)
            q = (vm_num - sis_info[0]) * 0.7 / (sis_info[1] + 1)
            tree_quota = pow(2, math.ceil(math.log(q)/math.log(2))) - 1; # the size of a tree is 2^n-1
            layers = int(math.log(tree_quota + 1, 2)) # how many layers there are in the tree
            
            name_replace = {}
            
            if tasks[in_node].is_parallel == True and tasks[in_node].is_reduce== False: # pred is parallel
                pred_quota = after_expand[in_node]
                if check_more_pred(edges, out_node) < 2: # the only edge: P->PR
                    if 2 ** (layers - 1) == pred_quota:
                        tree_quota -= 2 ** (layers - 1)
                        layers -= 1
                else:
                    if out_node in after_expand.keys(): # there is a tree already created
                        tree_quota = after_expand[out_node]
                        layers = int(math.log(tree_quota + 1, 2))

                after_expand[out_node] = tree_quota

                index_start = 0
                    
                
                for l in range(layers): # l是层数
                    single_quota = 2 ** (layers - l - 1) # 第0层有2^(3-0-1) = 4 个node
                    if l == 0 and layers != 1: # if first layer
                        each_reduce = int(pred_quota / single_quota)
                    
                        for i in range(single_quota):
                            # 自己的index是i
                            for j in range(each_reduce):
                                new_edges.append(Parser.Edge(in_node + str(i * each_reduce + j), out_node + "_" +str(l) + "_" + str(i+1)))
                                
                    elif l != layers - 1: # 中间layer
                        for i in range(single_quota):
                            new_edges.append(Parser.Edge(out_node + "_" + str(l-1) + "_" + str(2 * i + 1), out_node + "_" + str(l) + "_" + str(i+1)))
                            new_edges.append(Parser.Edge(out_node + "_" + str(l-1) + "_" + str(2 * i + 2), out_node + "_" + str(l) + "_" + str(i+1)))
                    
                    elif layers > 1: # last layer, name remains the same
                        new_edges.append(Parser.Edge(out_node + "_" + str(l-1) + "_" + str(1), out_node))
                        new_edges.append(Parser.Edge(out_node + "_" + str(l-1) + "_" + str(2), out_node))
                    
                    else:
                        each_reduce = int(pred_quota / single_quota)
                    
                        for i in range(single_quota):
                            # 自己的index是i
                            for j in range(each_reduce):
                                new_edges.append(Parser.Edge(in_node + str(i * each_reduce + j), out_node + "_" +str(l) + "_" + str(i+1)))
                        
                
                    for i in range(single_quota): # append the tasks
                        newt = copy.deepcopy(tasks[out_node])

                        if newt.name not in [i.name for i in new_tasks]:
                            if l != layers - 1:
                                newt.name += str(index_start + i)#str(l) + str(i)
                            newt.index = index_start + i
                            new_tasks.append(newt) 
                    index_start += single_quota
                    

                  
                    if l != layers - 1 and layers != 1:

                        upper_index = 0
                        for pl in range(0, l):
                            upper_index += 2 ** (layers - pl - 1)
                        for j in range(1, single_quota+1):
                            name_replace[out_node + "_" + str(l) + "_" + str(j)] = out_node + str(upper_index + j - 1)
                    elif layers ==  1:
                        name_replace[out_node + "_0_1"] = out_node 
                        
                for e in new_edges:
                    if e.in_node in name_replace.keys():
                        e.in_node = name_replace[e.in_node]
                    if e.out_node in name_replace.keys():
                        e.out_node = name_replace[e.out_node]

                            
                            


            else: # pred is NULL, reduce, parallel reduce
                if out_node in after_expand.keys(): # there is a tree already created
                    tree_quota = after_expand[out_node]
                    layers = int(math.log(tree_quota + 1, 2))
                    
                index_start = 0
                for l in range(layers): # l是层数
                    single_quota = 2 ** (layers - l - 1) # 第0层有2^(3-0-1) = 4 个node
                    if l == 0: # if first layer
                        for i in range(single_quota):
                            new_edges.append(Parser.Edge(in_node, out_node+str(l) + str(i+1)))
                                
                    elif l != layers - 1: # 中间layer
                        for i in range(single_quota):
                            new_edges.append(Parser.Edge(out_node + str(l-1) + str(2 * i + 1), out_node + str(l) + str(i+1)))
                            new_edges.append(Parser.Edge(out_node + str(l-1) + str(2 * i + 2), out_node + str(l) + str(i+1)))
                    
                    else: # last layer, name remains the same
                        new_edges.append(Parser.Edge(out_node + str(l-1) + str(1), out_node))
                        new_edges.append(Parser.Edge(out_node + str(l-1) + str(2), out_node))
                
                    for i in range(single_quota): # append the tasks
                        newt = copy.deepcopy(tasks[out_node])
                        if newt.name not in [i.name for i in new_tasks]:
                            if l != layers - 1:
                                newt.name += str(l) + str(i+1)
                            newt.index = index_start + i
                            new_tasks.append(newt) 
                    index_start += single_quota
                after_expand[out_node] = tree_quota

                            
            
    #for nt in new_tasks:
        #nt.display()
    #for ne in new_edges:
        #ne.display()
            
    return new_tasks, new_edges


def rebalance(new_tasks, new_edges):
    # clear the preds and succs
    for t in new_tasks:
        t.preds.clear()
        t.succs.clear()
    
    for e in new_edges:
        for t in new_tasks:
            if e.in_node == t.name:
                if e.out_node not in t.succs:
                    t.succs.append(e.out_node)
            elif e.out_node == t.name:
                if e.in_node not in t.preds:
                    t.preds.append(e.in_node)
                

    return new_tasks

def begin_expansion(vm_num, root, tasks, edges):
    task_dict = list_to_dict(tasks)
    new_tasks, new_edges = graph_expansion(vm_num, "A", task_dict, edges)
    new_tasks = rebalance(new_tasks, new_edges)
    return new_tasks, new_edges


if __name__ == "__main__": 
    filepath = "/Users/verasun/Desktop/capstone/version2/temp_file.txt"
    tasks, edges = Parser.run_parser(filepath)


    new_tasks, new_edges = begin_expansion(24, "A", tasks, edges)
    for t in new_tasks:
        t.display()
    for e in new_edges:
        e.display()


    


  
        
        
        
        