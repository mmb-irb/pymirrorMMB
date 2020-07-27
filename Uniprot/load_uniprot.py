#!/usr/bin/env python2
#encoding: UTF-8

# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

import argparse
import logging
import os
import gzip
import sys

from mmb_data.mongo_db_connect import Mongo_db
from mmb_data.mongo_db_bulk_write import CTS, MongoDBBulkWrite


BATCH_SIZE = 50000

def process_fasta(header, seq):

    data = header.split(' ')

    db,uniq_id,entry_name = data[0].split('|')
    
    header_data = {}
    
    prev_field = 'protein_name'
    header_data[prev_field] = []
    for i in range(1, len(data)):
        if data[i].find('=') == -1:
            header_data[prev_field].append(data[i])
        else:
            label, val = data[i].split('=')
            prev_field = label
            header_data[prev_field] = [val]
    
    header_data = {k: ' '.join(header_data[k]) for k in header_data}
    
    header_data['db']= db.replace('>','')
    header_data['_id'] = uniq_id
    header_data['swpId'] = entry_name
    header_data['header'] = header
    header_data['isoform'] = (uniq_id.find('-') != -1)
    
    if header_data['OX'] not in sources:
        sources[header_data['OX']] = header_data['OS']
        db_cols['sources'].insert_one({'_id': header_data['OX'], 'source': header_data['OS']})
        logging.info ("New source found: {}".format(header_data['OS']))
    header_data['source'] = header_data['OX']
    del header_data['OX']
    del header_data['OS']
    
    seqBuff.append(
        {'_id': header_data['_id']},
        {'$set' : 
            {
                '_id' : header_data['_id'],
                'sequence' : seq,
                'type' :  'protein',
                'origin' :'uniprot'
            }
        },
        header_data['_id']
    )

    headBuff.append(
        {'_id' : header_data['_id']},
        {'$set': header_data},
        header_data['_id']
    )
    if header_data['isoform']:
        refId, n = header_data['_id'].split('-')
        headBuff.append (
            {'_id': refId}, 
            {'$addToSet' : {'isoforms': header_data['_id']}}
        )
    headBuff.commit_data_if_full()
    seqBuff.commit_data_if_full()
    
    
def print_progress(prefix, nids, ntot, inc):
    ntot = max(1, ntot)
    if nids%inc == 0:
        logging.info("{} {:8}/{:8} {:5.1f}%".format(prefix, nids, ntot, (nids*100./ntot)))


cmd = argparse.ArgumentParser(
    description='Uniprot Fasta loader'
)

cmd.add_argument('--tupd', dest='tupd', action='store_true', required=False, help='New files only')
cmd.add_argument('--inic', dest='inic', required=False,help='Initial id to process')
cmd.add_argument('--ini_line', dest='ini_line', type=int, default=0, required=False, help='Initial line to process')
cmd.add_argument('--fin_line', dest='fin_line', type=int, default=0, required=False, help='Final line ot process')
cmd.add_argument('-v', dest='verb', action='store_true', required=False, help='Additional logging')
cmd.add_argument('--debug', dest='debug', action='store_true', required=False, help='Debug logging')
cmd.add_argument('files', nargs=argparse.REMAINDER, help="Files to process (FASTA(.gz))")
args = cmd.parse_args()

db_lnk = Mongo_db('localhost', 'FlexPortal', True, False)
db_cols = db_lnk.get_collections(["headers", "sequences", "sources","fileStamps"])

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')

headBuff = MongoDBBulkWrite(db_cols['headers'],CTS['UPSERT'], BATCH_SIZE)
seqBuff = MongoDBBulkWrite(db_cols['sequences'], CTS['UPSERT'], BATCH_SIZE)

if args.debug:
    logging.getLogger().setLevel(10)
else:
    logging.getLogger().setLevel(20)

logging.info('Reading sources...')

sources = {}
for sc in db_cols['sources'].find():
    sources[sc['_id']] = sc['source']
    
logging.info('{} sources loaded'.format(len(sources)))

ntot = db_cols['headers'].count_documents({})

logging.info('Reading input files...')

in_process = False
last_id = '-1'
nline = 0
nids = 0

for file in args.files:
    logging.info('Reading ' + file)
    file_stat = os.stat(file)
    tstamp =  int(file_stat.st_ctime)
    logging.info('File time stamp: {}'.format(tstamp))
    stored_tstamp = db_cols['fileStamps'].find_one({'_id':file})
    if args.tupd and stored_tstamp and (stored_tstamp['ts'] <= tstamp):
        logging.info("File not new, skipping")
        continue
    if file.find('.gz'):
        fh_in = gzip.open(file ,'r')
    else:
        fh_in = open(file ,'r')
    header = ''
    seq = ''
    for line in fh_in:
        nline += 1
        line = line.decode('ascii').rstrip()
        if not len(line):
            continue
        if line.find('>') == 0:
            nids  += 1
            in_process = in_process or\
                args.inic == last_id or (nline >= args.ini_line)
            if args.fin_line and in_process:
                in_process = (nline <= args.fin_line)
            if header and in_process:
                process_fasta(header, seq)
            header = line
            seq = ''
            print_progress('Input ids', nids, ntot, BATCH_SIZE)
        else:
            seq += line
        
    
    process_fasta(header, seq)
    
    headBuff.commit_any_data()
    seqBuff.commit_any_data()
    
    db_cols['fileStamps'].update_one({'_id':file},{'$set':{'ts':tstamp}})
    
logging.info('loadUniprot Done')

logging.info(headBuff.global_stats())

logging.info(seqBuff.global_stats())

