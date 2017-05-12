__author__ = 'yyao'

import sys
from functools import reduce
from multiprocessing import Pool

def Map(L):
    result = []
    for w in L:
        result.append(w[1:])
    return result

def load(path):
    word_list = []
    f = open(path,"r")
    for line in f:
        line = line.strip("'")
        word_list.append(line.strip().split(","))
    return word_list

def chunks(l,n):
    for i in range(0,len(l),n):
        yield l[i:i+n]

def Partition(L):
    sex_age = {}
    for sublist in L:
        for p in sublist:
            if p[0] in sex_age:
                sex_age[p[0]].append(int(p[1]))
            else:
                sex_age[p[0]] = []
                sex_age[p[0]].append(int(p[1]))
    return sex_age

def Reduce(Mapping):
    return (Mapping[0],sum(pair for pair in Mapping[1])/len(Mapping[1]))



if __name__ == '__main__':
    text =  load('table.txt')
    print(text)
    pool = Pool(processes=2,)
    print(pool)
    partitioned_text = list(chunks(text, len(text) // 2))
    print(partitioned_text)
    singlle = pool.map(Map,partitioned_text)
    print('SINGEL',singlle)
    token = Partition(singlle)
    print('Token:',token)
    term_ave = pool.map(Reduce,token.items())
    print(term_ave)
