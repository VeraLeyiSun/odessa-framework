#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 16 11:06:45 2021

@author: verasun
"""

import sys
import pickle

def count(input_file, prefix, index):
    punc = '''!()-[]{};:'"\, <>./?@#$%^&*_~'''
    """
    try:
        f = open(input_file, 'rb') # opens the corresponding file
        data = pickle.load(f) # read all lines of data in that file, store it as list and append to the list of all data
    except FileNotFoundError:
        return
    """
    
    f = open(input_file, 'rb') # opens the corresponding file
    data = pickle.load(f) # read all lines of data in that file, store it as list and append to the list of all data

    word_maps = [] # the rest of this funtion is the same as normal word count
    for word in data:
        word = word.strip()
        if word[-1] in punc: 
            word = word[:-1]
        word_maps.append([word, 1])
    f_new = open("{}_{}_output.txt".format(prefix, index), 'wb') # open corresponding file in append mode
    pickle.dump(word_maps, f_new)# write the line of data to corresponding file
    f.close() # close the file
    f_new.close()



if __name__ == '__main__':
    count(sys.argv[1], sys.argv[2], sys.argv[3])
    #count("A_0_recved.txt", "a", "b")
    #with open('a_b_output.txt', "rb") as infp:
    #    data = pickle.load(infp)
    #    print(data)

