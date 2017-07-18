"""
A very simple implementation of the wordcount example with a map-reduce.
"""
from itertools import groupby
from operator import itemgetter
import re

class Executor(object):
    """ Simulates a facade that implements a map and a reduce. So the
        implementations can be easyly redifined.
    """
    def map(self, function, values):
        return map(function, values)

    def reduce(self, function, values):
        return reduce(function, values)

lambda_executor = Executor()

# Input comes as a file.
with open('file.txt') as f:
    lines = f.readlines()


def mapper(line):
    """ Map function definition. Splits the lines and generates key-value for
        each word.
    """
    counts = {}
    line = line.lower()
    line = line.replace('.', '').replace(',', '').replace(':', '')
    words = line.split()
    for word in words:
        counts[word] = counts.get(word, 0) + 1
    return counts

# MAP APPLY
counters = lambda_executor.map(mapper, lines)
# print counters

# MAP DONE

# generate tuples (word, count) for each result
# FIXME: do this inside the map function so here just flat the list
counts = [(word, dic[word]) for dic in counters for word in dic.keys()]
counts.sort()   # sort the words so can be grouped
groups = groupby(counts, itemgetter(0))

def reducer(wordcount1, wordcount2):
    """ Reduce function definition. Add. """
    return wordcount1[0], wordcount1[1] + wordcount2[1]

# distribute the groups in a list of lists that contains (word,count) for each word
# each group is a word
x = [[(word, count) for word, count in group] for current_word, group in groups]

# REDUCE APPLY for each word, each word has its own reducer.
# TODO: Given a number of reducers, distribute words among them.
results = [lambda_executor.reduce(reducer, y) for y in x]
print results
