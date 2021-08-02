#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 16 11:34:26 2021

@author: verasun
"""
import sys
import pickle

def aggregate(input_file, prefix, index):
    try:
        f = open(input_file, 'rb') # opens the corresponding file
    except FileNotFoundError:
        return
    data = pickle.load(f)

    agg_result = []
    exist_word = []
    for i in data:
        if i[0] in exist_word:
            for j in agg_result:
                if j[0] == i[0]:
                    j[1] += i[1]
        else:
            exist_word.append(i[0])
            agg_result.append([i[0], i[1]])
            
    f_new = open("{}_{}_output.txt".format(prefix, index), 'wb') # open corresponding file in append mode
    pickle.dump(agg_result, f_new) # write the line of data to corresponding file
    f.close() # close the file
    f_new.close()

if __name__ == '__main__':
    aggregate(sys.argv[1], sys.argv[2], sys.argv[3])
