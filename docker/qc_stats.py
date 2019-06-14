from sgpy.aws import s3
import zipfile
import os
import re
import ast
import utils
import argparse


def main():
    """
    1. Download FastQC report from S3
    2. Parse out relevant metrics
    3. Compile into a pandas df on S3
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", dest='output', action='store', required=True)
    parser.add_argument("-l", dest='lib_info', action='store', required=True)
    args = parser.parse_args()

    # print to logs
    lib_info = ast.literal_eval(args.lib_info)
    print('Running qc stats script with:')
    print(lib_info)
    print('Outputting to:')
    print(args.output)

    # get stats from the various reports
    fastqc_stats = get_fastqc_stats(lib_info['fastqc'])
    summ_stats = get_summary_stats(lib_info['statistics'], lib_info['libbc'])
    fastqc_stats.update(summ_stats)

    # ouptut path
    out_pickle = lib_info['libbc'] + '_QC_stats.pkl'
    utils.do_the_pickle(fastqc_stats, out_pickle)
    s3.cp_local_to_s3(out_pickle, os.path.join(args.output, out_pickle))

    return


def get_fastqc_stats(zip_archives):
    matches = {}

    # download FASTQC zip archives and unzip
    for file in zip_archives:
        fn_path = s3.cp_s3_to_local(file, dest='/scratch/')
        zip_ref = zipfile.ZipFile(fn_path, 'r')
        zip_ref.extractall('/scratch/')
        zip_ref.close()

        # read the interesting file
        stats_f = os.path.join(fn_path.split('.zip')[0], 'fastqc_data.txt')
        with open(stats_f, 'r') as f:
            contents = f.read()

        # find metrics
        re_dup = re.compile('Total Deduplicated Percentage\t(\d\d\.\d)')
        re_qual = re.compile('Sequences flagged as poor quality\t(\d+)')
        re_adapt = re.compile('Adapter Content\t(\w+)')
        re_kmer = re.compile('Kmer Content\t(\w+)')
        re_base = re.compile('Per base sequence quality\t(\w+)')
        re_sscore = re.compile('Per sequence quality scores\t(\w+)')
        re_len = re.compile('Sequence Length Distribution\t(\w+)')
        re_overrep = re.compile('Overrepresented sequences\t(\w+)')

        for regexp in [re_dup, re_qual,re_adapt, re_kmer, re_base, re_sscore, re_len,
                       re_overrep]:
            key, val = regexp.search(contents).group(0).split('\t')
            if key not in matches:  # check if empty
                matches[key] = [val]
            else:
                matches[key] += val,

    return matches


def get_summary_stats(path, libbc):
    fn_path = s3.cp_s3_to_local(path, dest='/scratch/')
    with open(fn_path, 'r') as f:
        contents = f.readlines()
    stats = {}
    contents = [c.split(',') for c in contents if libbc in c]

    for row in contents:
        step = row[1]
        if step in stats:
            stats[step] += int(row[3].strip()),
        else:
            stats[step] = [int(row[3].strip())]

    metrics = {
        'PreQC': stats['0100'][0],
        'QualFilter_Adaptor_rm': sum(stats['0200'][1:]),
        'Contaminant_rm': stats['0300'][3] + stats['0300'][5],
        'Host_rm': stats['0500'][3] + stats['0500'][5],
        'PostQC':stats['0500'][3] + stats['0500'][5]
    }

    return metrics


def get_spades_stats(path):
    fn_path = s3.cp_s3_to_local(path, dest='/scratch/')
    with open(fn_path, 'r') as f:
        contents = f.readlines()
    stats = {}
    for l in [c.strip().split(':') for c in contents]:
        stats[l[0]] = l[1].strip()

    return stats


if __name__ == '__main__':
    main()