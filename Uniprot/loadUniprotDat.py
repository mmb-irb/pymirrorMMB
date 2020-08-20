#!/usr/bin/env python3
#encoding: UTF-8

from Bio import SwissProt
import pprint

import argparse
import logging
import os
import gzip
import sys

from mmb_data.mongo_db_connect import Mongo_db
from mmb_data.mongo_db_bulk_write import CTS, MongoDBBulkWrite
from mmb_data.file_mgr import FileMgr
import mmb_data.utils as ut

BATCH_SIZE = 1000
AUTH = False

cmd = argparse.ArgumentParser(
    description='Uniprot Fasta loader'
)

cmd.add_argument('--tupd', dest='tupd', action='store_true', required=False, help='New files only')
cmd.add_argument('--inic', dest='inic', required=False, help='Initial id to process')
cmd.add_argument('--skip_ids', dest='skip', type=int, required=False, default=0, help='Skip records')
cmd.add_argument('-v', dest='verb', action='store_true', required=False, help='Additional logging')
cmd.add_argument('--debug', dest='debug', action='store_true', required=False, help='Debug logging')
cmd.add_argument('files', nargs=argparse.REMAINDER, help="Files to process (FASTA(.gz))")

args = cmd.parse_args()

db_lnk = Mongo_db('localhost', 'FlexPortal', False, AUTH)
db_cols = db_lnk.get_collections(["headers", "sequences", "sources","Annotation", "uniprotFull", "fileStamps"])

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')

headBuff = MongoDBBulkWrite(db_cols['uniprotFull'],CTS['UPSERT'], BATCH_SIZE)

if args.debug:
    logging.getLogger().setLevel(10)
else:
    logging.getLogger().setLevel(20)

logging.info('Reading sources...')

sources = {}
for sc in db_cols['sources'].find():
    sources[sc['_id']] = sc['source']
    
logging.info('{} sources loaded'.format(len(sources)))

ntot = db_cols['headers'].estimated_document_count()

logging.info('Found {} documents'.format(ntot))

if args.skip:
    logging.info('Skipping {} records'.format(args.skip))
if args.inic:
    logging.info('Skipping until {}'.format(args.inic))

logging.info('Reading input files...')

in_process = not args.inic and not args.skip

nids = 0

pp = pprint.PrettyPrinter(indent=4)

for file in args.files:
    logging.info('Processing ' + file)
        
    f_mgr = FileMgr(file, 0, 0)

    if args.tupd and not f_mgr.check_stamp(db_cols['fileStamps']):
        logging.info("File not new, skipping")
        del f_mgr
        continue
    
    f_mgr.open_file()
    
    for rec in SwissProt.parse(f_mgr.fh_in):
        rec._id = rec.accessions[0]
        del rec.references
        headBuff.append(
            {'_id': rec._id},
            {'$set': vars(rec)}
        )
        headBuff.commit_data_if_full()
    
    headBuff.commit_any_data()
    
logging.info('loadUniprotFull Done')

logging.info(headBuff.global_stats())

logging.info(seqBuff.global_stats())


