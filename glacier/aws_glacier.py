#!/usr/bin/env python
"""Provides a command line interface for managing AWS Glacier archives.
It includes multipart file upload and basic functionalities to manage the Glacier vault.
"""

import requests, argparse, json, logging, sys, os, sha256_tree_hash, codecs
import subprocess, es_data_import
from pylog import PyLog
from datetime import datetime

__author__ = "Emanuele Disco"
__copyright__ = "Copyright 2017"
__license__ = "GPL"
__version__ = "1.0.0"
__email__ = "emanuele.disco@gmail.com"
__status__ = "Production"

BACKUP_TEMP_FOLDER = '/tmp'
PART_SIZE = 134217728  # 128M need to be power of 2
AWS_VAULT = os.getenv('AWS_VAULT')
ES_METADATA_INDEX = os.getenv('ES_INDEX')
ES_METADATA_TYPE = 'archive'
AWS_PATH = '/root/.local/bin/aws'
REMOVE_CHUNKS = False
GLACIER_DATA = '/usr/share/glacier/data'
ARCHIVE_PREFIX = '{}/archive.part_'.format(BACKUP_TEMP_FOLDER)

_args = {}
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s')
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

output = PyLog(filename=GLACIER_DATA + '/output.log', write_freq=1)

def _start_request():
    out = subprocess.check_output([AWS_PATH, 
                        "glacier", 
                        "initiate-multipart-upload", 
                        "--vault-name={}".format(AWS_VAULT), 
                        "--account-id=-", 
                        "--archive-description=test", 
                        "--part-size=%s" % PART_SIZE ])
    return json.loads(out.decode('UTF-8'))['uploadId']

def _complete_request(filename, upload_id, checksum):
    stats = os.stat(filename)
    archive_size = stats.st_size
    out = subprocess.check_output([AWS_PATH, 
                        "glacier", 
                        "complete-multipart-upload", 
                        "--vault-name={}".format(AWS_VAULT), 
                        "--account-id=-",
                        "--checksum=%s" % checksum,
                        "--upload-id=%s" % upload_id,
                        "--archive-size=%s" % archive_size 
                        ])
    return out.decode('UTF-8')

def _log_to_es(data, description="", filename=""):
    now = datetime.now()
    data = json.loads(data)

    data.update({ 
        "description": data.get('ArchiveDescription', description), 
        "filename": filename, 
        "timestamp": data.get('CreationDate', now.strftime('%Y-%m-%dT%H:%M:%S.%f%z'))
    })
    _logger.debug(data)

    response = es_data_import.post( ES_METADATA_INDEX, ES_METADATA_TYPE, data )
    return response.text

def _multi_upload(filename, upload_id):
    stats = os.stat(filename)
    archive_size = stats.st_size
    parts = archive_size // PART_SIZE
    if archive_size % PART_SIZE > 0:
        parts += 1
        
    sha256_parts = []
    dd = subprocess.Popen(['dd', 'if=%s' % filename, 'bs=2M'], stdout=subprocess.PIPE )
    #gzip = subprocess.Popen(['gzip', '-1', '-'], stdin=dd.stdout, stdout=subprocess.PIPE)
    out = subprocess.check_output(['split', 
        '--suffix-length=2', 
        '--numeric-suffixes=0', 
        '--bytes=%s' % PART_SIZE, '-', ARCHIVE_PREFIX], 
        stdin = dd.stdout )
    
    start=0
    end=0
    for i in range(parts):
        part_filename = ARCHIVE_PREFIX + ('%02d' % i)
        stats = os.stat(part_filename)
        end = start + stats.st_size - 1
        
        chunks = sha256_tree_hash.get_chunks_sha256_hashes(part_filename)
        checksum = sha256_tree_hash.compute_sha256_tree_hash(chunks)
        sha256_parts.append(codecs.getdecoder('hex')(checksum)[0])
        
        subprocess.call([AWS_PATH,
              'glacier', 
              'upload-multipart-part',
              '--vault-name={}'.format(AWS_VAULT),
              '--account-id=-',
              '--body', part_filename,
              '--upload-id', str(upload_id),
              '--checksum', checksum,
              '--range', 'bytes {}-{}/*'.format(start, end)])
        start = start + PART_SIZE

        if REMOVE_CHUNKS:
            os.remove(part_filename)

    complete_checksum = sha256_tree_hash.compute_sha256_tree_hash( sha256_parts )
    return complete_checksum

def upload(filename, description):
    upload_id = _start_request()
    _logger.debug('Requested Upload Id: {}'.format(upload_id))
    checksum = _multi_upload(filename, upload_id)
    _logger.debug('Full sha256 tree hash: {}'.format(checksum))
    aws_response = _complete_request(filename, upload_id, checksum)
    _logger.debug('Upload Complete request response: {}'.format(aws_response))
    es_response = _log_to_es(aws_response, filename=filename, description=description)
    _logger.debug('ES import response : {}'.format(es_response))

    return aws_response

def register_vault_list(filename):
    stats = os.stat(filename)
    if stats.st_size > 16777216:
        raise Exception('File size way too big...')
    file = open(filename)
    L = json.load(file)
    
    for l in L['ArchiveList']:
        _log_to_es(json.dumps(l))

def delete(archive):
    subprocess.call([AWS_PATH,
        'glacier', 
        'delete-archive',
        '--vault-name={}'.format(AWS_VAULT),
        '--account-id=-',
        '--archive-id={}'.format( archive )
    ])

def list_inventory():
    out = subprocess.check_output([AWS_PATH,
        'glacier', 
        'initiate-job',
        '--vault-name={}'.format(AWS_VAULT),
        '--account-id=-',
        '--job-parameters={"Type": "inventory-retrieval"}'
    ])
    return out.decode('UTF-8')

def jobs():
    out = subprocess.check_output([AWS_PATH,
        'glacier', 
        'list-jobs',
        '--vault-name={}'.format(AWS_VAULT),
        '--account-id=-'
    ])
    return out.decode('UTF-8')

def job(jobid):
    out = subprocess.check_output([AWS_PATH,
        'glacier', 
        'get-job-output',
        '--vault-name={}'.format(AWS_VAULT),
        '--account-id=-',
        '--job-id={}'.format(jobid),
        GLACIER_DATA + '/job_output'
    ])
    return out.decode('UTF-8')

def _main():
    if _args.register:
        register_vault_list(GLACIER_DATA + '/' + _args.file)
    elif _args.file:
        out = upload(GLACIER_DATA + '/' + _args.file, _args.descr)
        output.write(out)
    elif _args.delete:
        delete(_args.delete)
    elif _args.job:
        out = job(_args.job)
        output.write(out)
    elif _args.jobs:
        out = jobs()
        output.write(out)
    elif _args.list:
        out = list_inventory()
        output.write(out)

if __name__=='__main__':   
    _parser = argparse.ArgumentParser()
    _parser.add_argument('-f', action='store', dest='file', type=str, help='the file to upload without full path')
    _parser.add_argument('-m', action='store', dest='descr', type=str, help='description to upload')
    _parser.add_argument('-r', '--register', action='store_true', dest='register', help='register vault list to ES')
    _parser.add_argument('-v', '--verbose', action='store_true', dest='debug', help='verbose mode')
    _parser.add_argument('-d', '--delete', action='store', dest='delete', type=str, help='delete an archive')
    _parser.add_argument('--jobs', action='store_true', dest='jobs', help='retrieve job list')
    _parser.add_argument('-j', '--job', action='store', dest='job', type=str, help='retrieve a job output')
    _parser.add_argument('-l', '--archive-list', action='store_true', dest='list', help='retrieve the archive list')
    _parser.add_argument('-O', '--to-stdout', action='store_true', dest='stdout', help='print output to stdout')

    _args = _parser.parse_args()
    
    if _args.stdout:
        output = sys.stdout

    if _args.debug:
        _logger.setLevel(logging.DEBUG)
    
    _main()