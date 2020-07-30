import argparse
import logging
import os
import gzip
import sys
import re

from mmb_data.mongo_db_connect import Mongo_db
from mmb_data.mongo_db_bulk_write import CTS, MongoDBBulkWrite

BATCH_SIZE = 100000
AUTH = True


cmd = argparse.ArgumentParser(
    description='Uniprot Secondary Accession loader'
)

cmd.add_argument('--tupd', dest='tupd', action='store_true', required=False, help='New files only')
cmd.add_argument('--ini_line', dest='ini_line', type=int, default=0, required=False, help='Initial line to process')
cmd.add_argument('--fin_line', dest='fin_line', type=int, default=0, required=False, help='Final line ot process')
cmd.add_argument('-v', dest='verb', action='store_true', required=False, help='Additional logging')
cmd.add_argument('--debug', dest='debug', action='store_true', required=False, help='Debug logging')
cmd.add_argument('files', nargs=argparse.REMAINDER, help="Files to process (FASTA(.gz))")
args = cmd.parse_args()

db_lnk = Mongo_db('localhost', 'FlexPortal', False, AUTH)
db_cols = db_lnk.get_collections(["headers", "sequences", "Annotation", "fileStamps"])

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')

if args.debug:
    logging.getLogger().setLevel(10)
else:
    logging.getLogger().setLevel(20)

logging.info("Markng deleted entries")

headDelBuff = MongoDBBulkWrite(db_cols['headers'],CTS['DELETE'], BATCH_SIZE)
headUpdBuff = MongoDBBulkWrite(db_cols['headers'],CTS['UPDATE'], 3*BATCH_SIZE)
seqBuff = MongoDBBulkWrite(db_cols['sequences'], CTS['DELETE'], BATCH_SIZE)
annotBuff = MongoDBBulkWrite(db_cols['Annotation'], CTS['DELETE'], BATCH_SIZE)


if args.ini_line:
    logging.info('Starting at {} line'.format(args.ini_line))
if args.fin_line:
    logging.info('Stopping at {} line'.format(args.fin_line))

logging.info('Reading input files')

in_process = False
last_id = '-1'
nline = 0
nids = 0

for file in args.files:
    logging.info('processing ' + file)
    logging.info('Reading ' + file)
    file_stat = os.stat(file)
    tstamp =  int(file_stat.st_ctime)
    logging.info('File time stamp: {}'.format(tstamp))
    stored_tstamp = db_cols['fileStamps'].find_one({'_id':file})
    if args.tupd and stored_tstamp and (stored_tstamp['ts'] <= tstamp):
        logging.info("File not new, skipping")
        continue
    if file.find('.gz') != -1:
        fh_in = gzip.open(file ,'r')
    else:
        fh_in = open(file ,'r')
    
    header_lines = True
    for line in fh_in:
        header_lines = header_lines and (line.find('_____') == -1)
        if header_lines:
            continue
        if line.find('_____') != -1:
            continue
        line = line.rstrip()
        nline += 1
        if args.fin_line and nline > args.fin_line:
            break
        if nline > args.ini_line:
            if not len(line):
                break
            id = line
            for buff in headDelBuff, seqBuff, annotBuff:
                buff.append({'_id': id},{})
                buff.commit_data_if_full()
            
            for ref in 'UniRef100','UniRef90','UniRef50':
                headUpdBuff.append(
                    {'dbxref.'+ref: id},
                    {'$pullAll':{'dbxref.'+ ref: [id]}}
                )

            headUpdBuff.commit_data_if_full(True);

    for buff in headDelBuff, seqBuff, annotBuff:
        buff.commit_any_data()
    headUpdBuff.commit_any_data(True)

    db_cols['fileStamps'].update_one({'_id':file},{'$set':{'ts':tstamp}}, upsert=True)

logging.info('Mark deleted Done')

logging.info(headDelBuff.global_stats())

logging.info(headUpdBuff.global_stats())

logging.info(seqBuff.global_stats())

logging.info(annotBuff.global_stats())

