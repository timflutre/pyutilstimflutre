# -*- coding: utf-8 -*-
# Wraps code from sqlite3

# Copyright (C) 2016 Institut National de la Recherche Agronomique (INRA)
# License: GPL-3+
# Persons: Timothée Flutre [cre,aut]
# Versioning: https://github.com/timflutre/pyutilstimflutre

from __future__ import print_function
from __future__ import unicode_literals

import os
import sqlite3


class DbSqlite(object):
    """
    Wraps code from sqlite3.
    """
    
    def __init__(self, path2db):
        if os.path.exists(path2db):
            msg = "db '%s' already exists" % path2db
            raise ValueError(msg)
        self.db = path2db
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        
    def execute(self, cmd):
        self.cur.execute(cmd)
        
    def commit(self):
        self.conn.commit()
        
    def execomm(self, cmd):
        self.execute(cmd)
        self.commit()
        
    def doesTableExist(self, table):
        cmd = "PRAGMA table_info(\"%s\");" % table
        self.execute(cmd)
        res = self.cur.fetchall()
        return len(res) > 0
        
    def getColumnList(self, table):
        cmd = "PRAGMA table_info(\"%s\");" % table
        self.execute(cmd)
        res = self.cur.fetchall()
        if not len(res) > 0:
            msg = "table '%s' doesn't exist" % table
            raise ValueError(msg)
        return [col[1] for col in res]
