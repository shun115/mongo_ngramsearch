# -*- coding: utf-8 -*-
import os
from common import ngram
from pymongo import Connection,database,collection
from ConfigParser import SafeConfigParser

conf = SafeConfigParser()
conf.read(os.path.join(os.path.dirname(__file__), "app.config"))


def main():
    conn = Connection(conf.get('mongo','HOST'),
                      int(conf.get('mongo','PORT')))
    srcdb = database.Database(conn, conf.get('search','SOURCE_DB'))
    srccoll = collection.Collection(srcdb,
                                    conf.get('search','SOURCE_COLL'))
    docs = []
    for r in srccoll.find():
        docs.append({'docid': r[conf.get('search','DOCID')],
                     'contents': r[conf.get('search','CONTENTS')]})

    inverted = {}
    for d in docs:
        tokens = ngram(d['contents'],
                       int(conf.get('search','INDEX_TYPE')))
        for t in tokens:
            if not t[0] in inverted:
                inverted[t[0]] = [(d['docid'], t[1])]
            else:
                inverted[t[0]].append( [d['docid'], t[1]] )

    inddb = database.Database(conn, conf.get('search','INDEX_DB'))
    indcoll = collection.Collection(inddb,
                                    conf.get('search','INDEX_COLL'))
    indcoll.drop()
    for i in inverted.items():
        indcoll.insert({'word': i[0],
                        'position': i[1]})
    indcoll.ensure_index('word')

    metacoll = collection.Collection(inddb,
                                    conf.get('search','INDEX_META_COLL'))
    metacoll.drop()
    metacoll.insert({'doccount': len(docs)})


if __name__ == '__main__':
    main()
