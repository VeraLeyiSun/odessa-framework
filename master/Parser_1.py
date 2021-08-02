#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 10 19:04:00 2021

@author: verasun
"""

"""
Parser 1st version
"""

"""
the class Task stores the information and status of each task in the DAG
"""

import os.path # check if function executables/files exist

class Task():
    def __init__(self, name):
        self.name = name
        self.prefix = name
        self.index = None
        self.func = None
        self.is_parallel = False
        self.is_reduce = False
        self.row_i = None
        self.col_i = None
        self.succs = []
        self.preds = []
        self.state = "idle"
        self.machineID = None
    
    def set_func(self, func_name):
        self.func = func_name
    
    def set_parallel(self):
        self.is_parallel = True
    
    def set_reduce(self):
        self.is_reduce = True
    
    def set_row_i(self, row_index):
        self.row_i = row_index
    
    def set_col_i(self, col_index):
        self.col_i = col_index
        
    def add_successor(self, other):
        self.succs.append(other)
        
    def add_predecessor(self, other):
        self.preds.append(other)
    
    def set_state(self, s):
        self.state = s
        
    def set_machine_ID(self, mid):
        self.machineID = mid
        
    def display(self):
        #print("Name: {}\n Function: {}\n Successors: {}\n Predecessors: {}\n State: {}\n".format(self.name, self.func,  self.succs, self.preds, self.state))
        print("Name: {}\nmachine: {}\nState: {}\nPrefix: {}\nIndex: {}\nPreds: {}".format(self.name, self.machineID, self.state, self.prefix, self.index, str(self.preds)))


class Edge():
    def __init__(self, in_ver, out_ver):
        self.in_node = in_ver
        self.out_node = out_ver
    
    def display(self):
        print("{} -> {}".format(self.in_node, self.out_node))



def run(task, text):

    s1, s2, lparen, rparen, comma, newline = -1, -1, -1, -1, -1, -1
    #row_index, col_index = 0, 0
    s1 = text.index(" ")
    s2 = text.index(" ", s1+1)
    func_name = text[s1+1 : s2]
    

    if os.path.isfile(func_name):
        task.set_func(func_name)
    else:
        print("FileNotFoundError: No such file: '{}'".format(func_name))
        return -1


    i = 0
    for i in range(s2+1, len(text)):
        if text[i] == "(":
            lparen = i
        elif text[i] == ",":
            comma = i
        elif text[i] == ")":
            rparen = i
        elif text[i] == "\n":
            newline = i
            break
        i += 1
    
    # error: unclosed braces
    if lparen + rparen < abs(lparen) + abs(rparen):
        print("SyntaxtError: unexpected EOF while parsing")
    # error: incomplete (row_i, column_i)
    if comma == -1:
        print("ValueError: invalid input indices")
        
    if "max" in text[lparen+1 : comma]:
        task.set_row_i("max")
    else: # error: invalid row index
        try:
            #row_i = int(text[lparen+1 : comma])
            task.set_row_i(int(text[lparen+1 : comma]))
        except ValueError:
            print("ValueError: invalid row index")

    if "max" in text[comma+1 : rparen]:
        task.set_col_i("max")
    else:
        try: # error: invalid column index
            task.set_col_i(int(text[comma+1 : rparen]))
        except ValueError:
            print("ValueError: invalid column index")
    #print(text)
    #print(s1, s2, lparen, rparen, comma, newline)
    #print("----------------------------")
    return newline

def annotate(task, text):
    at = -1
    #at, start, stop = -1, -1, -1
    i = 0
    while text[i] != "@":
        i += 1
    if "parallel" in text[at+1 : ]:
        task.set_parallel()
    if "reduce" in text[at+1 : ]:
        task.set_reduce()
    

def create_task(text):

    task_i = text.index("task")
    n1 = text.index(" ", task_i + 1)
    n2 = text.index(" ", n1 + 1)
    name = text[n1+1 : n2]
    this_task = Task(name)
    lbrace, rbrace, run_i = -1, -1, -1
    
    lbrace = text.index("{")
    rbrace = text.index("}")
    
    for i in range(n2+1, len(text)):
        #print(text[i], i)
        if text[i] == "{":
            pass
            #lbrace = i
        elif text[i] == "}":
            #rbrace = i
            break
        elif text[i: i+3] == "run":
            
            run_end = run(this_task, text[i:]) + 8 + len(name)
            if run_end == -1:
                return -1, -1

        elif text[i] == "@":
            annotate(this_task, text[i:rbrace])
    return this_task, rbrace

def create_edge(text):
    edge_i, arrow, newline = text.index("edge"), text.index("->"), text.index("\n")
    pred_name = text[edge_i+4 : arrow].strip()
    succ_name = text[arrow+2 : newline].strip()
    return pred_name, succ_name, newline


def run_parser(filepath):
    f = open(filepath, "r")
    text = f.read()
    tasks = []
    edges = []

    i=0
    while i < len(text):
        if text[i : i+4] == "task":
            this_task, closing = create_task(text[i:])
            if this_task == -1:
                """
                Error checking
                """
                return -1, -1
            tasks.append(this_task)
            i += closing
        elif text[i : i+4] == "edge":
            pred, succ, closing = create_edge(text[i:])
            edges.append(Edge(pred, succ))
            exist_flag = False
            for t in tasks:
                if t.name == pred:
                    exist_flag = True
                    t.add_successor(succ)
                if t.name == succ:
                    t.add_predecessor(pred)
            if not exist_flag:
                print("NameError: task '{}' is not defined".format(pred)) #两个都得查！
            i += closing 
        else:
            i += 1
    return tasks, edges



if __name__ == "__main__":
    filepath = "/Users/verasun/Desktop/capstone/version2/temp_file.txt"
    tasks, edges = run_parser(filepath)
    for t in tasks:
        t.display()
    for e in edges:
        e.display()
    

