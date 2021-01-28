""" Import Ensembl variants"""

import argparse
import logging
import os
import gzip
import sys

from mmb_data.mongo_db_connect import Mongo_db
from mmb_data.mongo_db_bulk_write import CTS, MongoDBBulkWrite
from mmb_data.file_mgr import FileMgr
import mmb_data.utils as ut

BATCH_SIZE = 10000
AUTH = True

cmd = argparse.ArgumentParser(
    description='Uniprot Variants loader'
)

cmd.add_argument('--tupd', dest='tupd', action='store_true', required=False, help='New files only')
cmd.add_argument('--inic', dest='inic', required=False, help='Initial id to process')
cmd.add_argument('--skip_ids', dest='skip', type=int, required=False, default=0, help='Skip records')
cmd.add_argument('--ini_line', dest='ini_line', type=int, default=0, required=False, help='Initial line to process')
cmd.add_argument('--fin_line', dest='fin_line', type=int, default=0, required=False, help='Final line ot process')
cmd.add_argument('-v', dest='verb', action='store_true', required=False, help='Additional logging')
cmd.add_argument('--debug', dest='debug', action='store_true', required=False, help='Debug logging')
cmd.add_argument('files', nargs=argparse.REMAINDER, help="Files to process")
args = cmd.parse_args()

db_lnk = Mongo_db('localhost', 'FlexPortal', False, AUTH)
db_cols = db_lnk.get_collections(["variants", "fileStamps"])

logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')

varBuff = MongoDBBulkWrite(db_cols['variants'],CTS['UPSERT'], BATCH_SIZE)

if args.debug:
    logging.getLogger().setLevel(10)
else:
    logging.getLogger().setLevel(20)

if args.skip:
    logging.info('Skipping {} records'.format(args.skip))
if args.inic:
    logging.info('Skipping until {}'.format(args.inic))
if args.ini_line:
    logging.info('Starting at line {}'.format(args.ini_line))
if args.fin_line:
    logging.info('Stopping at line {}'.format(args.fin_line))

logging.info('Reading input files...')

for file in args.files:
    logging.info('Processing ' + file)
        
    f_mgr = FileMgr(file, args.ini_line, args.fin_line)

    if args.tupd and not f_mgr.check_new_stamp(db_cols['fileStamps']):
        logging.info("File not new, skipping")
        del f_mgr
        continue
    
    f_mgr.open_file()
    
    if args.inic:
        logging.info("Skipping until {} ".format(args.inic))
        f_mgr.skip_lines_to(args.inic, True)
    else:
        f_mgr.skip_lines_to('_________ __________ ___________ ______________ _____________ ______________ _____________________', True)
    f_mgr.skip_lines_to_ini()
    
    for line in f_mgr:
        if not len(line):
            break
#A1BG      P04217     VAR_018369  p.His52Arg     Polymorphism  rs893184    -
#A1BG      P04217     VAR_018370  p.His395Arg    Polymorphism  rs2241788   -
        data = line.split()
        #gn,unipId, $varId, $mut,$type,$dbId,@data) = split ' ';
        varBuff.append(
            {'_id': {'ac': data[1], 'mut':data[3]}},
            {
                '$set' : {
                    'gn': data[0],
                },
                '$addToSet': {
                    'variants': {
                        'varorig':'humsavar', 
                        'varId': data[2],
                        'type': data[4],
                        'dbId' : data[5],
                        'phenotype' : ' '.join(data[6:]),
                    }
                }
            }
        )
      
        logging.debug("{} {} {}".format(f_mgr.current_line, data[1], data[2]))
        varBuff.commit_data_if_full(True)

    varBuff.commit_any_data(True)

    db_cols['fileStamps'].update_one({'_id':file},{'$set':{'ts':f_mgr.tstamp}}, upsert=True)
    
    del f_mgr

logging.info('HUmsavar Variants Done')

logging.info(varBuff.global_stats())
