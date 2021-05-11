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
AUTH = False

AA3Let = {
    'A': 'Ala', 'C': 'Cys', 'D': 'Asp', 'E': 'Glu', 'F': 'Phe', 'G': 'Gly',
    'H': 'His', 'I': 'Ile', 'K': 'Lys', 'L': 'Leu', 'M': 'Met', 'N': 'Asn',
    'P': 'Pro', 'Q': 'Gln', 'R': 'Arg', 'S': 'Ser', 'T': 'The', 'V': 'Val',
    'Y': 'Tyr'
}

transl_fields = {
    'gene_affected' : 'ENSG',
    'transcript_affected': 'ENST',
    'consequence_type': 'consequence',
    'chromosome': 'Chrom',
    'aa_mutation': 'mut', 
    'icgc_mutation_id': 'dbId'
}

del_fields = [
    'submitted_sample_id', 
    'submitted_matched_sample_id', 
    'verification_status', 
    'verification_platform', 
    'biological_validation_status', 
    'biological_validation_platform', 
    'gene_build_version', 
    'platform', 
    'experimental_protocol', 
    'sequencing_strategy', 
    'base_calling_algorithm', 
    'alignment_algorithm', 
    'variation_calling_algorithm', 
    'other_analysis_algorithm', 
    'seq_coverage', 
    'initial_data_release_date',
    'matched_icgc_sample_id'
]

cmd = argparse.ArgumentParser(
    description='ICGC Variants loader'
)

cmd.add_argument('--tupd', dest='tupd', action='store_true', required=False, help='New files only')
cmd.add_argument('-v', dest='verb', action='store_true', required=False, help='Additional logging')
cmd.add_argument('--debug', dest='debug', action='store_true', required=False, help='Debug logging')
cmd.add_argument('files', nargs=argparse.REMAINDER, help="Files to process")
args = cmd.parse_args()

db_lnk = Mongo_db('localhost', 'FlexPortal', False, AUTH)
db_cols = db_lnk.get_collections(["headers", "variants", "fileStamps"])

logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')

varBuff = MongoDBBulkWrite(db_cols['variants'],CTS['UPSERT'], BATCH_SIZE)

if args.debug:
    logging.getLogger().setLevel(10)
else:
    logging.getLogger().setLevel(20)

logging.info('Reading input files...')

for file in args.files:
    logging.info('Processing ' + file)

    f_mgr = FileMgr(file)

    if args.tupd and not f_mgr.check_new_stamp(db_cols['fileStamps']):
        logging.info("File not new, skipping")
        del f_mgr
        continue

    f_mgr.open_file()

    line = f_mgr.__next__()
    
    fields = line.split('\t')
    
    for line in f_mgr:
        print(line)
        if not len(line):
            break
        data = line.split('\t')

        dataObj = {}
        
        for i,v in enumerate(fields):
            if fields[i] not in del_fields:
                print(fields[i], data[i])
                if fields[i] in transl_fields:
                    dataObj[transl_fields[fields[i]]] = data[i]
                else:
                    dataObj[fields[i]] = data[i] 
        print(dataObj)
        
        sys.exit()

#        varBuff.append(
#            {'_id': {'ac': data[1], 'mut':data[3]}},
#            {
#                '$set' : {
#                    'gn': data[0],
#                },
#                '$addToSet': {
#                    'variants': {
#                        'varorig':'humsavar',
#                        'varId': data[2],
#                        'type': data[4],
#                        'dbId' : data[5],
#                        'phenotype' : ' '.join(data[6:]),
#                    }
#                }
#            }
#        )

        logging.debug("{} {} {}".format(f_mgr.current_line, data[1], data[2]))
        varBuff.commit_data_if_full(True)

    varBuff.commit_any_data(True)

    db_cols['fileStamps'].update_one({'_id':file},{'$set':{'ts':f_mgr.tstamp}}, upsert=True)

    del f_mgr

logging.info('HUmsavar Variants Done')

logging.info(varBuff.global_stats())
