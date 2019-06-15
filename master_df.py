from sgpy.aws import s3
import os
from utils import get_pairs, unpickle, base_path
import csv
import pandas as pd

# paths
target_path = 's3://somewhere-on-s3/summary_stats/'
pairs = get_pairs()

# process OTU annotation file
extracts = [e for e in pairs]
edf = pd.read_csv(base_path + 'extracts.csv')
keep_cols = ['Biospecimen type', 'Study site ID', 'Extract Barcode', 'Extract type',
            'OTU_Diversity', 'nr_plasmid']
edf = edf.filter(keep_cols)
edf.set_index('Extract Barcode', inplace=True)
edf = edf[edf.index.isin(extracts)]


def dl_and_unpickle(out_pickle):
    d = s3.cp_s3_to_local(out_pickle, dest='/scratch/')
    obj = unpickle(d)
    os.remove(d)
    return obj


def generate_stats_csv():
    header =['extract_bc',
             'lib_bc',
             'lib_type',
             'deduplicated_pct',
             'poor_qual_count',
             'raw_reads',
             'qualfilt_rm_reads',
             'conta_rm_reads',
             'host_rm_reads',
             'filt_reads',
             'adapter_content',
             'kmer_content',
             'per_base_seq_qual',
             'per_seq_qual_score',
             'seq_len_distrib',
             'over_rep_seqs',
             'biospec_type',
             'Study_site_ID',
             'Extract_type',
             'OTU_Diversity',
             'nr_plasmid',
             'Total_contigs',
             'N50',
             'L50',
             'N95',
             'L95',
             'gene_count',
             'intersection_count']

    with open(outf, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(header)

        for i, extract in enumerate(pairs):
            if i % 10 == 0:
                print('Processed %i extracts' %i)

            pair = pairs[extract]
            pair_stats = {}
            md5s = {}

            for lib_type in ['SMTG', 'DMTG']:
                libbc = pair[lib_type]['libbc']
                qc = dl_and_unpickle(pair[lib_type]['qc_data'])
                md5s[lib_type] = dl_and_unpickle(pair[lib_type]['gene_md5'])

                stats = [
                    extract,
                    libbc,
                    lib_type,
                    sum([float(e) for e in qc['Total Deduplicated Percentage']]) / 3.0,  # avg out across R1, R2 and unpaired FASTQs
                    sum([float(e) for e in qc['Sequences flagged as poor quality']]) / 3.0,
                    qc['PreQC'],
                    qc['QualFilter_Adaptor_rm'],
                    qc['Contaminant_rm'],
                    qc['Host_rm'],
                    qc['PostQC']
                ]

                # compute metrics between R1/R2
                for metric in ['Adapter Content', 'Kmer Content', 'Per base sequence quality',
                               'Per sequence quality scores', 'Sequence Length Distribution',
                               'Overrepresented sequences']:
                    s = get_fastqc_level(qc[metric])
                    stats += s,

                # add metadata, OTU
                stats.extend(edf.loc[edf.index == extract].values.tolist()[0])

                # add gene count from prodigal
                stats += len(md5s[lib_type]),
                pair_stats[lib_type] = stats

            #calculate gene intersection between the 2 lib types
            inter = len(md5s['SMTG'].intersection(md5s['DMTG']))
            for lt in pair_stats:
                lib_stats = pair_stats[lt]
                lib_stats += inter,
                csv_writer.writerow(lib_stats)

    s3.cp_local_to_s3(outf, target_path + os.path.basename(outf))
    print('Done.')
    return


def get_fastqc_level(metric_list):
    m = set(metric_list)
    if len(m) == 1:
        s = list(m)[0]
    elif 'fail' in m:
        s = 'fail'
    elif 'warn' in m:
        s = 'warn'
    else:
        s = 'pass'

    return s


if __name__ == '__main__':
    outf = base_path + 'summary.csv'
    generate_stats_csv()
