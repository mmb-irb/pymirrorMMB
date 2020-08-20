import logging

def print_progress(prefix, nids, ntot, inc):
    ntot = max(1, ntot)
    if nids%inc == 0:
        logging.info("{} {:8}/{:8} {:5.1f}%".format(prefix, nids, ntot, (nids*100./ntot)))


def get_id(fasta_header):
    db, uniq_id, entry_name = fasta_header.split('|')
    return uniq_id
