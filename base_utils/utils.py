#!/usr/bin/env python

'''
@author yniknafs
'''

import os
import sys
import argparse
import logging
import collections
import shutil
import tempfile
import multiprocessing
import inspect
import marshal, types
from multiprocessing import Pool


class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

def chunks(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]

class ERROR(Exception):
    pass

def mkdir(dirname):
    dirname = os.path.abspath(dirname)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

class MultiprocessCounter(object):
    def __init__(self, total, name):
        self.running = multiprocessing.Manager().Value('i', 0)
        self.togo = multiprocessing.Manager().Value('i', total)
        self.name = name

    def __str__(self):
        lineo = '*'*10 + ' ' + '(%s) runs counter -- %d running ; %d to go' % \
            (self.name, int(self.running.value), int(self.togo.value)) + \
            ' ' +  '*'*10 + '\r'
        return lineo

    def log(self):
        lineo = str(self)
        sys.stdout.write(lineo)
        sys.stdout.flush()

    def finish(self):
        self.running.value-=1
        self.togo.value-=1

    def start(self):
        self.running.value+=1

class TmpDir(object):
    def __init__(self, path=None):
        if path: mkdir(path)
        self.path = tempfile.mkdtemp(dir=path)
        self.files = multiprocessing.Manager().dict()
        self.numsubdirs = {}
        self.subdirs = {}
        self.osfiles = []

    def __str__(self):
        return self.path

    def rm(self):
        for n in self.osfiles:
            os.close(n)
        shutil.rmtree(self.path)


    def mkfile(self, name=None):
        n, f = tempfile.mkstemp(dir=self.path)
        self.osfiles.append(n)
        if name is not None:
            if name in self.files:
                raise ERROR('TMP file "%s" already exists in TMP_DIR' % name)
            self.files[name] = f
        else:
            self.files[len(self.files)] = f
        return f


def getMethodAttrs(c):
    return inspect.getmembers(c, lambda a:not(inspect.isroutine(a)))


def parallelImap(argtuple):
    code_string, args = argtuple
    
    code = marshal.loads(code_string)
    func = types.FunctionType(code, globals(), "parallel_func")
    return func(*args)


def runP(paramsWFunc, cores):

    output = []

    procs = int(cores)
    pool = Pool(processes=procs)
    result_iter = pool.imap_unordered(parallelImap, paramsWFunc)
    for i, result in enumerate(result_iter):
        logging.debug("	finished sample %s: %s with return code" % (i, str(result)))
        output.append(result)
    pool.close()

    return output

def addFuncToParams(func, params):
    code_string = marshal.dumps(func.func_code)
    params = [(code_string, x) for x in params]
    return params

# Import this function for running parallel functions
def runFuncParallel(func, params, cores):
    paramsWFunc = addFuncToParams(func, params)
    result = runP(paramsWFunc, cores)
    return result

# function to calculate how to divy up cores for a 
# multiprocessing run that kicks off multi core jobs
def multiProcCores(totalCores, taskLen):
    if taskLen >= totalCores:
                cores = 1
    else:
        cores = int(totalCores/float(taskLen))
    return cores