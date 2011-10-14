# -*- coding: utf-8 -*-


def ngram(text, n):
    results = []
    if len(text) >= n:
        for i in xrange(len(text)-n+1):
            results.append((text[i:i+n], i+1))
    return results
