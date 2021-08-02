#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 17 13:25:17 2021

@author: verasun
"""
import random
import math


def generate_graph(node_dict):
    # node_dict: {task name: # left}
    edges = []
    vms = list(node_dict.keys())
    backup_vms = vms[:]

    for k, v in node_dict.items():
        inedge = v
        backup_vms = vms[:]
        backup_vms.remove(k)
        neighbor = random.sample(backup_vms, inedge)
        for n in neighbor:
            edges.append((n, k))
    return vms, edges

    
def choose_direction(curr_node, edges):
    neighbors = []
    for e in edges:
        if e[0] == curr_node:
            neighbors.append(e[1])
    if len(neighbors) > 0:
        return random.choice(neighbors)
    else:
        return None
    

def remove_edge(vm, edges):
    es = []
    for e in edges:
        if e[1] == vm:
            es.append(e)
    e = random.choice(es)
    edges.remove(e)
    return e
    

def add_edge(v, vms, edges):
    back_up = vms[:]
    back_up.remove(v)
    u = random.choice(back_up)
    while (u, v) in edges:
        u = random.choice(back_up)
    edges.append((u, v))
    return (u, v)
    

def walk(thresh, vms, edges, curr_node): #recursively simulate the walk process
    #print("current node:", curr_node)
    if thresh == 0: #in simulating the walk task, which task is not important(?
        return curr_node #then this node executes the task
    next_node = choose_direction(curr_node, edges)
    if next_node == None:
        """
        backup = vms[:]
        backup.remove(curr_node)
        next_node = random.choice(backup)
        """
        backup = []
        for e in edges:
            if e[1] not in backup:
                backup.append(e[1])
        if curr_node in backup:
            backup.remove(curr_node)
        next_node = random.choice(backup)
    return walk(thresh - 1, vms, edges, next_node)


def pick_another_one(banned, vms, edges):
    back_up = vms[:]
    for dup in banned:
        back_up.remove(dup)
    tobe = []
    for e in edges:
        if e[1] in back_up:
            tobe.append(e[1])
    return random.choice(tobe)
   


def in_master(thresh, vm_dict):
    # initially: create the graph
    vms, edges = generate_graph(vm_dict)
    edges = [('204', '198'), ('200', '198'), ('204', '203'), ('200', '203'), ('201', '204'), ('210', '204'), ('205', '201'), ('206', '201'), ('210', '202'), ('209', '202'), ('210', '200'), ('209', '200'), ('200', '206'), ('210', '206'), ('207', '209'), ('205', '209'), ('207', '208'), ('200', '208'), ('204', '210'), ('208', '210'), ('202', '205'), ('208', '205'), ('205', '207'), ('203', '207')]
    print(edges)
    for i in range(8):
        start_node = random.choice(vms) # randomly在graph上pick一个起始点
        machine = walk(thresh, vms, edges, start_node)
        print(i, machine)
        remove_edge(machine, edges)
        print(edges)
        print()
    

    
    
if __name__ == "__main__": 
    vm_dict = {'198': 2, "203": 2, "204": 2, "201": 2,
               "202": 2, "200": 2, "206": 2, "209": 2,
               "208": 2, "210": 2, "205": 2, "207": 2}
    thresh = math.ceil(math.log(len(vm_dict))) #calculate threshold
    in_master(thresh, vm_dict)
        
    
