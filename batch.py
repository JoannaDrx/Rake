import boto3
import utils
import yaml
from sgpy.aws import s3


def main():

    spades_runs = 0
    prodigal_runs = 0
    md5_runs = 0
    qc_data_runs = 0
    full_libs = 0

    # looks for missing paths in the pairs df and runs the corresponding job
    pairs = utils.get_pairs()
    for extract in pairs:

        pair = pairs[extract]
        for lib_type in ['SMTG', 'DMTG']:
            try:
                pair_data = pair[lib_type]
            except KeyError:  # incomplete pair, skip
                continue

            missing = [path for path in pair_data if pair_data[path] == 'NA']
            try:
                assert all(e not in missing for e in ['pipeline_log', 'fastqc', 'qced_fastq', 'statistics'])
            except AssertionError:
                print('ERROR:\n', str(pair))
                continue

            if 'qc_data' in missing:
                qc_data_runs += 1
                run_batch(pair_data, 'qc_data')

            if 'spades_assembly' in missing and 'gene_md5' in missing:
                spades_runs += 1
                out_f = pair_data['qced_fastq'][0].split('2_005_Processed')[0] + '2_100_Assembly_By_Samples/'
                fold = [f for f in s3.ls(out_f, recursive=True) if pair_data['libbc'] in f][0]
                out = fold.rsplit('/', 1)[0] if not fold.endswith('/') else fold
                run_batch(pair_data, 'spades', output_path=out)

            elif 'prodigal_out' in missing:
                    prodigal_runs += 1
                    run_batch(pair_data, 'prodigal')

            elif 'gene_md5' in missing:
                md5_runs += 1
                run_batch(pair_data, 'md5')

            if not missing:
                full_libs += 1

    print('Pairs:',len(pairs))
    print('Full libs:', full_libs)
    print('SPAdes jobs:', spades_runs)
    print('Prodigal jobs:', prodigal_runs)
    print('MD5 hash jobs: ', md5_runs)
    print('FastQC parsing jobs: ', qc_data_runs)

    return


def run_batch(lib_data, job_type, output_path=None):

    # get params
    with open('batch.yml', 'r') as f:
        config = yaml.load(f)
    config = config[job_type]

    # build command line
    job_name = lib_data['libbc'] + '_' + job_type
    job_def = config['job_definition']
    cpus = config['cpu']
    mem = config['memory']
    duration = config['duration']


    if not output_path:
        output_path = config['output_path']

    if job_type == 'prodigal':  # legacy container with different cmd structure
        cmd = [
            "-c", str(cpus),
            "-n", job_name,
            lib_data['spades_assembly'],  # does this need to be a list?
            output_path
        ]

    else:
        cmd = ['python',
               config['script'],
               "-l", str(lib_data),
               "-o", output_path
               ]

    _batch(job_name, job_def, cmd, cpus, mem, duration)
    return


def _batch(jobname, jobdef, cmd, cpus, memory, duration):
    batch_client = boto3.client('batch')
    batch_client.submit_job(jobName=jobname, jobQueue='Queue-name-here',
                            jobDefinition=jobdef,
                            containerOverrides={'command': cmd,
                                                'vcpus': cpus,
                                                'memory': memory
                                                },
                            timeout={'attemptDurationSeconds': duration})
    return


if __name__ == '__main__':
    main()