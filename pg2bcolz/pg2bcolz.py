#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2018, Stas Fomin <stas-fomin@yandex.ru>

import itertools
import psycopg2
import numpy as np
import bcolz
import gzip
import datetime
import cachetools


import itertools


def sql2bcolz(sql, dsfilename, con, type_hints={}):
    """
    Read SQL query, return a DataFrame.

    Parameters
    ----------
    sql : SQL string with all parameters substituted
    con : connectable (django connection, or psycopg connection) 
    """
    
    # We have to estimate number of rows for one-time allocation of numpy arrays
    
    count = None
    sql_count = "select count(*) from (%s) s" % sql

    cursor = con.cursor()
    cursor.execute(sql_count)
    count = cursor.fetchone()[0]
    cursor.close()

    if count == 0:
        return None

    # Funny way to reliable get psycopg connection. We need it to get server-side cursors.
    pgcon = con.cursor().connection
    
    # with transaction.atomic():
    if True:
        cursor = pgcon.cursor("serversidecursor", withhold=True)
        pgcon.commit()
        chunk_size = 100000
        if count:
            chunk_size = int(max(min(count/10, 100000), 10))
        cursor.itersize = chunk_size

        print(str(datetime.datetime.now()), "Start executing query …  ")
        cursor.execute(sql)
        print(str(datetime.datetime.now()), "End executing query …  ")
        row0 = cursor.fetchone()
        print(str(datetime.datetime.now()), "End fetch first row …  ")
        columns = []
        dtypes = []

        def ResultIter(cursor):
            while True:
                results = cursor.fetchmany(chunk_size)
                if not results:
                    break
                for result in results:
                    yield result

        for i, col_desc in enumerate(cursor.description):
            col_name = col_desc[0]
            dtype = None
            if col_name == 'agg_color_model_ids':
                iii = 1
                pass
            if col_name in type_hints:
                dtype = type_hints[col_name]
            else:    
                if col_desc.type_code == 25:
                    dtype = 'S16'
                    pass
                if col_desc.type_code == 16:
                    dtype = 'bool'
                elif col_desc.type_code == 1043:
                    dtype = 'S16'
                    pass
                elif col_desc.type_code == 1082:
                    dtype = 'i8'
                elif col_desc.type_code in [700, 701]:
                    dtype  = 'f%d' % col_desc.internal_size
                elif col_desc.type_code == 1016:
                    dtype  = '(128,)i8'
                elif col_desc.type_code == psycopg2.NUMBER:
                    size_ = col_desc.internal_size
                    if size_ < 0:
                        size_ = 8
                    dtype  = 'i%d' % size_
            columns.append(col_name)
            dtypes.append( (col_name, dtype) )

        # ct = bcolz.fromiter(ResultIter(cursor), dtype=dtypes, count=count, rootdir=dsfilename)
        ct = bcolz.fromiter(cursor, dtype=dtypes, count=count, rootdir=dsfilename)
        cursor.close()
        del cursor
    pass





