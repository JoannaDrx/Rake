import os
import utils
from sgpy.aws import s3  # common aws utils package


def get_paths(pairs):
    """
    For each pair, finds the path to relevant files on S3:
        - '0_600_Post_QC_FastQC/*.zip (R1|R2)'   # qc information
        - '2_100_Assembly_By_Samples/*.spades.contigs.fasta   # spades assembly file
        - '0_000_Logfiles/pipeline.txt'  # to get the plasmid or mtg flag for spades
        - '2_005_Processed_Reads/*R1|R2|unpaired.fastq.gz  # QC'ed FASTQs to run spades on
        - 's3://some-location/here/*_prodigal.ffn'  # prodigal output with special Connor setting
        - '2_300_Gene_Prediction/*.ffn'  # post-assembly post-combined fasta - may not be relevant
        - '2_350_Gene_Cluster_Counts/*counts.txt'  # assembled genes - may not be relevant
        - 's3://some-location/here/*_md5_hashes.pkl'  # pickle of prodigal gene call md5s
    """

    for i, extract in enumerate(pairs):
        print('Gathering paths for pair # %i of %i extract %s' % (i+1, len(pairs), extract))
        pair = pairs[extract]
        for lib_type in pair:  # one of SMTG, DMTG, V4
            print(f'    Processing {lib_type} paths...')
            libbc = pair[lib_type]['libbc']
            s3_path = pair[lib_type]['s3_path']  # pipeline output parent folder

            if lib_type in ['SMTG', 'DMTG']:
                target_paths = {
                    'pipeline_log': {
                        'folder': os.path.join(s3_path, '0_000_Logfiles/'),
                        'ls_kwargs': {
                            'suffix': 'pipeline.txt',
                            'expected': 1}
                    },

                    'statistics': {
                        'folder': os.path.join(s3_path, '0_000_Statistics/0600_Combined_Stats/'),
                        'ls_kwargs': {
                            'suffix': 'stats_summary.csv',
                            'expected': 1}
                    },

                    'fastqc': {
                        'folder': os.path.join(s3_path, '0_600_Post_QC_FastQC/'),
                        'ls_kwargs': {
                            'pattern': libbc,
                            'suffix': '.zip',
                            'expected': 3}
                    },

                    'qced_fastq': {
                        'folder': os.path.join(s3_path, '2_005_Processed_Reads/'),
                        'ls_kwargs': {
                            'pattern': libbc,
                            'suffix': '.fastq.gz',
                            'expected': 3}
                    },

                    'spades_assembly': {
                        'folder': os.path.join(s3_path, '2_100_Assembly_By_Samples/'),
                        'ls_kwargs': {
                            'pattern': libbc +'.*spades.contigs.fasta',
                            # don't use suffix here as some are gz'ed and others not
                            'recursive': True,
                            'expected': 1}
                    },

                    'assembly_qc': {
                        'folder': 's3://some-location/here',
                        'ls_kwargs': {
                            'pattern': libbc +'_assembly_stats.txt',
                            'expected': 1}
                    },

                    'assembly_qc2': {
                        'folder': os.path.join(s3_path, '2_100_Assembly_By_Samples/'),
                        'ls_kwargs': {
                            'pattern': libbc +'.*spades.contigs.stats',
                            'recursive': True,
                            'expected': 1}
                    },

                    'prodigal_out': {
                        'folder': 's3://some-location/here',
                        'ls_kwargs': {
                            'pattern': libbc,
                            'suffix': '_prodigal.ffn',
                            'expected': 1}
                    },

                    'gene_md5': {
                        'folder': 's3://some-location/here',
                        'ls_kwargs': {
                            'pattern': libbc,
                            'suffix': 'md5_hashes.pkl',
                            'expected': 1}
                    },

                    'qc_data': {
                        'folder': 's3://some-location/here',
                        'ls_kwargs': {
                            'pattern': libbc,
                            'suffix':  '_QC_stats.pkl',
                            'expected': 1}
                    },

                }

            for target in target_paths:
                print(f'        Working on {target}...', end='')
                kwargs = target_paths[target].get('ls_kwargs', {})

                try:
                    ls_results = s3.ls(target_paths[target]['folder'], **kwargs)
                    if kwargs['expected'] == 1:
                        ls_results = ls_results[0]
                    pair[lib_type][target] = ls_results
                    print('OK')

                except ValueError: # expected count not found
                    pair[lib_type][target] = 'NA'
                    print('ERROR')

    return pairs


if __name__ == '__main__':
    pairs = utils.get_pairs()
    pairs = get_paths(pairs)
    utils.save_obj_to_s3(pairs, 'pairs.pkl', s3_dest='s3://some-dest/here')
