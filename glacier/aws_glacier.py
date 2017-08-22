#!/usr/bin/env python

import requests, argparse, json, logging, sys, os, sha256_tree_hash, codecs
import subprocess, es_data_import
from pylog import PyLog
from datetime import datetime

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

pylog = PyLog(filename=GLACIER_DATA + '/output.log', write_freq=1)

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
    #es_response = _log_to_es(aws_response, filename=filename, description=description)
    #_logger.debug('ES import response : {}'.format(es_response))

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

def _main():
    if _args.register:
        register_vault_list(GLACIER_DATA + '/' + _args.file)
    elif _args.file:
        out = upload(GLACIER_DATA + '/' + _args.file, _args.descr)
        pylog.log(out)
    elif _args.delete:
        delete(_args.delete)

if __name__=='__main__':   
    _parser = argparse.ArgumentParser()
    _parser.add_argument('-f', action='store', dest='file', type=str, help='File to upload')
    _parser.add_argument('-m', action='store', dest='descr', type=str, help='Description')
    _parser.add_argument('-r', '--register', action='store_true', dest='register', help='Register vault list to ES')
    _parser.add_argument('-v', '--verbose', action='store_true', dest='debug', help='More logging on console')
    _parser.add_argument('-d', '--delete', action='store', dest='delete', type=str, help='Delete an archive')
    _args = _parser.parse_args()
    
    if _args.debug:
        _logger.setLevel(logging.DEBUG)
    
    _main()