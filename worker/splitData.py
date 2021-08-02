#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 20:06:16 2021

@author: verasun
"""

import sys
import pickle

def split(input_file, prefix, index):
    f = open(input_file, 'rb')
    print("1")
    words = pickle.load(f).split()
    f_new = open("{}_{}_output.txt".format(prefix, index), 'wb')
    pickle.dump(words, f_new)
    print("2")



if __name__ == '__main__':
    split(sys.argv[1], sys.argv[2], sys.argv[3])
    #split("input_0_recved.txt", "aaa", "bbb")
    #with open("aaa_bbb_output.txt", "rb") as fp:
        #d = pickle.load(fp)
        #print(d)
