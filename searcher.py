# -*- coding: utf-8 -*-
import os
import sys
import math
from common import ngram
from pymongo import Connection,database,collection
from ConfigParser import SafeConfigParser

conf = SafeConfigParser()
conf.read(os.path.join(os.path.dirname(__file__), "app.config"))


def main():
    q = sys.argv[1]
    q = unicode(q, 'utf-8')
    qs = ngram(q, int(conf.get('search','INDEX_TYPE')))

    tfs = {}
    scores = {}
    for _q in qs:
        if _q[0] in tfs:
            tfs[_q[0]] += 1
        else:
            tfs[_q[0]] = 1

    conn = Connection(conf.get('mongo','HOST'),
                      int(conf.get('mongo','PORT')))
    inddb = database.Database(conn, conf.get('search','INDEX_DB'))
    indcoll = collection.Collection(inddb,
                                    conf.get('search','INDEX_COLL'))
    metacoll = collection.Collection(inddb,
                                    conf.get('search','INDEX_META_COLL'))
    doccount = metacoll.find_one()['doccount']

    for w, tf in tfs.items():
        df = 0
        row = indcoll.find_one({'word': w})
        if row:
            df = len(row['position'])
        idf = math.log(doccount / (df + 1.0))
        tfidf = tf * idf

        if row:
            for docid, idx in row['position']:
                if docid in scores:
                    scores[docid] += tfidf
                else:
                    scores[docid] = tfidf

    result = []
    for k,v in sorted(scores.items(),
                      key=lambda x:x[1],
                      reverse=True):
        result.append((k,v))

    srcdb = database.Database(conn, conf.get('search','SOURCE_DB'))
    srccoll = collection.Collection(srcdb,
                                    conf.get('search','SOURCE_COLL'))
    for docid, score in result:
        print srccoll.find_one({'docid': docid})['contents']
        print score


if __name__ == '__main__':
    main()
