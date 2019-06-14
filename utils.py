import pickle
from sgpy.aws import s3
from hashlib import md5
import os

## relevant paths
if 'USER' in os.environ and os.environ['USER'] == 'joannadreux':
    base_path = '/Users/joannadreux/Desktop/goodScience/'
    print('Running locally')

else:
    os.environ['PYTHONPATH'] = '/home/ec2-user/'
    os.environ['OHSLAP'] = 'TRUE'
    base_path = '/dev/shm/goodScience/'
    print('Running in the clouuuud')

# colors
sg_hexes = [ '#84cf04',  '#01b5bb', '#fbb040', '#8fc742', '#832e3b', '#0eb9e2']

# key files
pairs_s3 = 's3://some-location/pairs.pkl'


def get_pairs():
    print('Downloading pairs')
    ps = s3.cp_s3_to_local(pairs_s3,  base_path+ 'pairs.pkl')
    pairs = unpickle(ps)
    return pairs


def save_obj_to_s3(obj, path, s3_dest):
    do_the_pickle(obj, path)
    s3.cp_local_to_s3(path, s3_dest)
    return


def do_the_pickle(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f)
    return


def unpickle(path):
    with open(path, 'rb') as pickled_obj:
        obj = pickle.load(pickled_obj)
    return obj


def get_hash(a):
    return md5(a.encode('utf-8')).hexdigest()
