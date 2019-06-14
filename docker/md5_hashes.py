from sgpy.aws import s3
from Bio import SeqIO
import os
import utils
import argparse
import ast


def main():
    """
    1. Download a FASTA file from S3
    2. Generate an md5 hash of each sequence in the FASTA
    3. Write the list of md5 to a pickle file on S3. The purpose of this file is to enable fast comparison
    of sequences at 100% identity across different samples.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", dest='output', action='store', required=True)
    parser.add_argument("-l", dest='lib_info', action='store', required=True)
    args = parser.parse_args()

    # get ouptut path
    lib_info = ast.literal_eval(args.lib_info)
    libbc = lib_info['libbc']
    out_pickle = libbc + '_md5_hashes.pkl'


    # print to logs
    print('Running md5 hashes script with:')
    print(lib_info)
    print('Outputting to:')
    print(args.output)

    # download prodigal output fasta
    fn_path = s3.cp_s3_to_local(lib_info['prodigal_out'], dest='/scratch/')
    with open(fn_path, 'r') as f:
        seqs = list(SeqIO.parse(f, 'fasta'))
    protein_hashes = set([utils.get_hash(str(a.seq)) for a in seqs])

    utils.do_the_pickle(protein_hashes, out_pickle)
    s3.cp_local_to_s3(out_pickle, os.path.join(args.output, out_pickle))

    return


if __name__ == '__main__':
    main()