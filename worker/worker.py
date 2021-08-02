#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 14:26:21 2021

@author: verasun
"""

import socket
import threading
import os
import pickle
import sys
import errno
from socket import error as socket_error

# Receive three types of messages
# from the Master
    # {type: "master", content: {prefix: task_prefix, index: i, func: .py, 
                                # preds: {prefix: [ips]}, num_suc: #}}
# from the peer for requesting
    # {type: request, want: peer's predecessor (self) name (prefix_index), 
    # for: successor name (pref_index)} 
    
# from the peer for file transfer
    # {type: file, to: self's name, source: predecessor name (prefix_index), data: data}

# from the Master:
    # {type: "complete"}

# Send three types of messages
# to the Master upon the task completion
    # {type: complete, task: prefix_index}
# to the peer for requesting
    # {type: request, want: pred's name, for: self's name}
# to the peer for sending the file
    # {type: file, to: successor's name, source: self's name (prefix_index), data: data}


TASK_QUEUE = []
# format: {prefix: task_prefix, index: i, func: .py, preds: {prefix: [ips]}, num_suc: #}
SEND_QUEUE = []
# format: {发送内容 + target: ip}
SEND_INDEX = {"input": 0}
# format: {task name (prefix_index): index}
RECV_INDEX = {}
# format: {predecessor's task: index}


#MASTER_IP = "192.168.2.199" # RAM shanghai
#MASTER_IP = "172.17.170.79" # self Beijng
#MASTER_IP = "192.168.1.225" # datacenter changed: Beijing

MASTER_IP = None

peer_ips = []


def read_ip_list(file):
    with open(file, "r") as f:
        ips = f.read().split()
    for i in ips:
        i = i.strip()
    
    d = {}
    for i in ips:
        d[i] = 2
    return ips, d



def recv():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # opens a socket and act as a client
    SELF_IP = socket.gethostname()
    s.bind((SELF_IP, 12345))
    s.listen(10)
    print("start recv thread...")
    i = 0
    while True:
        c,a = s.accept()
        whole_string = ''
        recv_data = ''

        #print("Receiving data from a[0] {}...".format(a[0]))
        """
        if "192.168.2" not in a[0]:
            print("Unknown peer.")
            continue
        """
        
        print(a[0], type(a[0]))
        print(MASTER_IP)
        print(a[0] == MASTER_IP)
        if a[0] != MASTER_IP and a[0] not in peer_ips:
            print("Unknown peer.")
            continue
        

        while True:
            recv_data = c.recv(1024).decode()

            #print("inside recving loop: {}".format(recv_data))
            
            if "?" in recv_data:
                index = recv_data.find("?")
                whole_string += recv_data[:index]
                break
            else:
                whole_string += recv_data
                

            
        #print("Finished recving before eval()...")
        msg = eval(whole_string)

        if msg["type"] == "master": 

            print("Received command from Master:", msg)
            # put a new task in the TASK_QUEUE
            TASK_QUEUE.append(msg["content"])
            # put requests in the SEND_QUEUE
            pred_info = msg["content"]["preds"]
            for k in pred_info.keys():
                for i in range(len(pred_info[k])):
                    task_name = msg["content"]["prefix"] + "_" + str(msg["content"]["index"])
                    RECV_INDEX[task_name] = 0
                    p = k + "_" + str(i+msg["content"]["start_index"])
                    d = {"type": "request", "want": p, "for": task_name, "target": pred_info[k][i]}
                    SEND_QUEUE.append(d)

                
            
        elif msg["type"] == "request":
            
            print("Received request from a peer:")
            target = a[0]

            want_task = msg["want"]
            succ = msg["for"]
            
            if want_task == "input_0":
                want_task = "input"
                
            i = SEND_INDEX[want_task]
            d = {"type": "file", "to": succ, "source": want_task, "index": i, "target": target}
            SEND_QUEUE.append(d)
            SEND_INDEX[want_task] += 1
            
        
        
        
        elif msg["type"] == "file":
            pred = msg["source"]
            for_task = msg["to"]
            print("Received file from task {} for task {}".format(pred, for_task))

            f = open(pred+ "_recved.txt",'wb')
            pickle.dump(msg["data"], f)
            f.close() # close this file after receiving all data
            RECV_INDEX[for_task] += 1;
            #print("received file stored")
        
        
        elif msg["type"] == "all complete": 
            print("Received finished signal from the master")
            TASK_QUEUE.clear()
            SEND_QUEUE.clear()
            SEND_INDEX.clear()
            SEND_INDEX["input"] = 0
            RECV_INDEX.clear()
            os.system("rm -f *_*")
            #print("after removing the file:")
            #print(TASK_QUEUE, SEND_QUEUE, SEND_INDEX, RECV_INDEX)
            

        c.close()


def send():
    while True:
        if SEND_QUEUE:
            s = socket.socket()
            send_task = SEND_QUEUE.pop(0)
            target_ip = send_task["target"]
            del send_task["target"]
            
            if send_task["type"] == "request":

                print("sending request to", target_ip)
                try:
                    s.connect((target_ip, 12345))
                    s.send((str(send_task) + "?").encode())

                except socket_error as serr:
                    if serr.errno != errno.ECONNREFUSED:
                        print("unknown error")
                    else: 
                        continue

                
                
                
            elif send_task["type"] == "complete":
                
                print("sending complete to master:{}".format(str(send_task)))

                             
                try:
                    s.connect((target_ip, 8899))
                    s.send((str(send_task)).encode())
                except socket_error as serr:
                    if serr.errno != errno.ECONNREFUSED:
                        print("unknown error")
                    else:
                        continue
            
            
            else: # send data
                #  send_task = {"type": "file", "to": succ name, "source": source,  "index": i}
                
                task = send_task["source"]
                if task != "input":
                    f = open(task + "_output_" + str(send_task["index"]) + ".txt", "rb")

                else:
                    f = open("input.txt", "rb")
                data = pickle.load(f)
                d = str({"type": "file", "to": send_task["to"], "source": send_task["source"], "data": data})



                print("sending file {} to a peer {}".format(task + "_output_" + str(send_task["index"]), send_task["to"]))
                
                try:
                    s.connect((target_ip, 12345))
                except socket_error as serr:
                    if serr.errno != errno.ECONNREFUSED:
                        print("unknown error")
                    else:
                        continue
                    
                sended_data = 0

                while sended_data < len(d):

                    if sended_data+1024 < len(d):
                        s.sendall(d[sended_data:sended_data+1024].encode())
                    else:
                        string = d[sended_data:len(d)] 
                        #print("last MB:", string)
                        s.sendall((string + "?").encode())
                        #print(d[sended_data:sys.getsizeof(data)])
                        #s.send((d[sended_data:sys.getsizeof(data)] + "finish???").encode())

                    sended_data += 1024
            print("sending finished")


def r(self_name, pred_prefix, start_index, length):
    if length == 1:
        if pred_prefix == "input":
            os.rename("input_recved.txt", self_name + '_input.txt')
        else:
            os.rename(pred_prefix + "_" + str(start_index) + "_recved.txt", self_name + '_input.txt')
        return
    
    data = ""
    for i in range(start_index, start_index + length):
        try:
            f = open(pred_prefix + "_" + str(i) + '_recved.txt', 'rb')
            sub_data = pickle.load(f)
            if i == start_index:
                if isinstance(sub_data, list):
                    data = []

            data += sub_data
            i += 1
        except FileNotFoundError:
            break

    with open(self_name + '_input.txt','wb') as f_new:
        pickle.dump(data, f_new)
    

    for i in range(start_index, start_index + length):
        os.system("rm -f " + pred_prefix + "_" + str(i) + '_recved.txt')

        
    return data


def w(self_name, num_succ):
    if num_succ == 0:
        return
    with open(self_name + '_output.txt', "rb") as infp:
        files = [open(self_name +'_output_%d.txt' % i, 'wb') for i in range(num_succ)]
        data = pickle.load(infp)
        split_data = []
        for i in range(num_succ):
            split_data.append([])
        for i in range(len(data)):
            split_data[i % num_succ].append(data[i])
        for i in range(num_succ):
            pickle.dump(split_data[i], files[i])
        for f in files:
            f.close()
    os.system("rm -f " + self_name + '_output.txt')
    


def user_func_handler():
    while True:
        if TASK_QUEUE:
            task_dict = TASK_QUEUE.pop(0)
            #print(task_dict)
            # {prefix: task_prefix, index: i, func: .py, 
                #preds: {prefix: [ips]}, num_suc: #}
            task_name = task_dict["prefix"] + "_" + str(task_dict["index"])
            pred_prefix = list(task_dict["preds"].keys())[0] # simplicity: only 1 predecessors
            start_index = task_dict["start_index"]
            
            while RECV_INDEX[task_name] != len(task_dict["preds"][pred_prefix]):
                pass
            
            #print("after loop before r()")
            r(task_name, pred_prefix, start_index, len(task_dict["preds"][pred_prefix])) # call read

            
            #print('python ' + task_dict["func"] + " " + task_prefix + "_" + str(task_dict["index"]) + '_input.txt')
            command = "python3 {} {} {} {}".format(task_dict["func"], task_name + '_input.txt',  task_dict["prefix"], str(task_dict["index"]))

            os.system(command)
            print(command)
            while not os.path.isfile(task_name + '_output.txt'):
                pass
            w(task_name, task_dict["num_succ"])
            #print("task {} is finished!".format(task_name))
            d = {"type": "complete", "task": task_name, "target": MASTER_IP}
            SEND_QUEUE.append(d)
            SEND_INDEX[task_name] = 0
                


"""

execution command: 
python3 worker.py master_ip ip_list.txt

"""
if __name__ == "__main__":
    MASTER_IP, ip_file = sys.argv[1], sys.argv[2]
    peer_ips = read_ip_list(ip_file)[0]

    recv_thread = threading.Thread(target = recv, args = ())
    send_thread = threading.Thread(target = send, args = ())
    task_thread = threading.Thread(target = user_func_handler, args = ())
    recv_thread.start()
    send_thread.start()
    task_thread.start()
                
    
