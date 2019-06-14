import os
import subprocess32
import shlex
from sgpy.aws import s3
import argparse
import multiprocessing
import psutil
import gzip
import shutil
import ast


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", dest='lib_info', action='store', required=True)
    parser.add_argument("-o", dest='output', action='store', required=True)
    args = parser.parse_args()

    # parse pipeline log to find out if mtg or plasmid run
    lib_info = ast.literal_eval(args.lib_info)
    libbc = lib_info['libbc']
    # print to logs
    print('Running Spades script with:')
    print(lib_info)
    print('Outputting to:')
    print(args.output)

    flag = parsePipelineFile(lib_info['pipeline_log'])


    # download the fastqs locally
    paths = lib_info['qced_fastq']
    fwd, rev, unp = [f for f in paths if 'fwd' in f][0], \
                    [f for f in paths  if 'rev' in f][0], \
                    [f for f in paths  if 'unpaired' in f][0]
    fwd = s3.cp_s3_to_local(fwd, '/scratch/')
    rev = s3.cp_s3_to_local(rev, '/scratch/')
    unp = s3.cp_s3_to_local(unp, '/scratch/')

    # set params
    cpus = multiprocessing.cpu_count() - 1
    ram = int(psutil.virtual_memory().total /1e9 - 0.1)

    # run SPAdes!
    cmd = "python /spades/bin/spades.py " \
          f"-1 {fwd} " \
          f"-2 {rev} " \
          f"-s {unp} " \
          f"-t {cpus} " \
          f"-m {ram} " \
          f"--{flag} " \
          "-o /scratch/tmp_spades_assembly/"
    # "-k 27,47,67,87,107,127 " \
    print('Running SPAdes with:\n', cmd)
    subprocess32.Popen(shlex.split(cmd)).wait()
    print('Done')

    # generate the out_path
    out_f = args.output

    # upload files
    paths = ['/scratch/tmp_spades_assembly/contigs.fasta',
             '/scratch/tmp_spades_assembly/scaffolds.fasta',
             '/scratch/tmp_spades_assembly/assembly_graph.fastg']
    for f in paths:
        print(f'Compressing {f}')
        comp_f = f + '.gz'
        with open(f, 'rb') as f_in:
            with gzip.open(comp_f, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print('Uploading %s' % comp_f)
        s3.cp_local_to_s3(comp_f,
                          os.path.join(out_f, libbc + '_spades.' +  os.path.basename(f) + '.gz'))

    return


def parsePipelineFile(logfile):
    # Read in the logfile
    cfg = s3.read(logfile)
    for line in cfg.split('\n'):
        if line.startswith('#'):
            break
        key,val = line.split('=')
        if key == 'IS_PLASMID':
            plas = val
            break
    return 'plasmid' if plas.lower().strip() == 'true' else 'meta'


if __name__ == '__main__':
    main()