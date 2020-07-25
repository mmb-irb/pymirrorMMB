#!/usr/bin/env python2
#encoding: UTF-8

# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

from mmb_data.mongo_db_connect import Mongo_db

if __name__ == "__main__":
    db_lnk = Mongo_db('localhost', 'FlexPortal', True, False)
    db_lnk.connect_db()

    headers_col = db_lnk.db.get_collection('headers')
    print(headers_col.find_one(no_cursor_timeout=True)['_id'])
    
