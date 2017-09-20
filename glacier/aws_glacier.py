#!/usr/bin/env python
"""Provides a command line interface for managing AWS Glacier archives.
It includes multipart file upload and basic functionalities to manage the Glacier vault.
"""

import argparse
import json
import logging
import sys
import os
import codecs
import subprocess
from datetime import datetime
import sha256_tree_hash
import es_data_import
from pylog import PyLog

__author__ = "Emanuele Disco"
__copyright__ = "Copyright 2017"
__license__ = "GPL"
__version__ = "1.0.0"
__email__ = "emanuele.disco@gmail.com"
__status__ = "Production"

BACKUP_TEMP_FOLDER = '/tmp'
PART_SIZE = int(os.getenv('PART_SIZE', 134217728))  # 128M need to be power of 2
AWS_VAULT = os.getenv('AWS_VAULT')
ES_METADATA_INDEX = os.getenv('ES_INDEX')
ES_METADATA_TYPE = os.getenv('ES_TYPE', 'archive')
AWS_PATH = '/root/.local/bin/aws'
GLACIER_DATA = '/usr/share/glacier/data'

_args = {}
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s')
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

output = PyLog(filename=GLACIER_DATA + '/output.log', write_freq=1)


def _start_request(descr=''):
    out = subprocess.check_output([AWS_PATH,
                                   "glacier",
                                   "initiate-multipart-upload",
                                   "--vault-name={}".format(AWS_VAULT),
                                   "--account-id=-",
                                   "--archive-description=\"{}\"".format(descr),
                                   "--part-size={}".format(PART_SIZE)
                                   ])
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

    response = es_data_import.post(ES_METADATA_INDEX, ES_METADATA_TYPE, data)
    return response.text

def _multi_upload(filename, upload_id):
    stats = os.stat(filename)
    archive_size = stats.st_size
    parts = archive_size // PART_SIZE
    if archive_size % PART_SIZE > 0:
        parts += 1
        
    sha256_parts = []
    start=0
    end=0
    for i in range(parts):
        part_filename = BACKUP_TEMP_FOLDER + '/archive.part'
        end = start + PART_SIZE - 1
        if end > archive_size:
            end = archive_size - 1
 
        _logger.debug('Sending part %s-%s', start, end)
        dd = subprocess.Popen(['dd', 
                          'if=%s' % filename,
                          'of=%s' % part_filename,
                          'bs=2M',
                          'skip=%s' % (start // (2*1048576)),
                          'count=%s' % (PART_SIZE // (2*1048576)),
                          'status=noxfer'
                         ])
        dd.wait()
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
                         '--range', 'bytes {}-{}/*'.format(start, end)
                        ])
        start = start + PART_SIZE
        os.remove(part_filename)  # remove temporary part file

    complete_checksum = sha256_tree_hash.compute_sha256_tree_hash( sha256_parts )
    return complete_checksum


def upload(filename, description):
    """Upload a single archive given the file fullpath and a description."""
    upload_id = _start_request(description)
    _logger.debug('Requested Upload Id: %s', upload_id)
    checksum = _multi_upload(filename, upload_id)
    _logger.debug('Full sha256 tree hash: %s', checksum)
    aws_response = _complete_request(filename, upload_id, checksum)
    _logger.debug('Upload Complete request response: %s', aws_response)
    es_response = _log_to_es(aws_response, filename=filename, description=description)
    _logger.debug('ES import response : %s', es_response)

    return aws_response


def register_vault_list(filename):
    """Register an archive list requested from Glacier service into the ElasticSearch engine."""
    stats = os.stat(filename)
    if stats.st_size > 16777216:
        raise Exception('File size way too big...')
    file = open(filename)
    L = json.load(file)

    for l in L['ArchiveList']:
        _log_to_es(json.dumps(l))


def delete(archive):
    """Delete an archive from the vault passing the archive id as argument."""
    subprocess.call([AWS_PATH,
                     'glacier',
                     'delete-archive',
                     '--vault-name={}'.format(AWS_VAULT),
                     '--account-id=-',
                     '--archive-id={}'.format(archive)
                     ])


def list_inventory():
    """Request the inventory list from the Glacier service."""
    out = subprocess.check_output([AWS_PATH,
                                   'glacier',
                                   'initiate-job',
                                   '--vault-name={}'.format(AWS_VAULT),
                                   '--account-id=-',
                                   '--job-parameters={"Type": "inventory-retrieval"}'
                                   ])
    return out.decode('UTF-8')


def jobs():
    """Request the current jobs list."""
    out = subprocess.check_output([AWS_PATH,
                                   'glacier',
                                   'list-jobs',
                                   '--vault-name={}'.format(AWS_VAULT),
                                   '--account-id=-'
                                   ])
    return out.decode('UTF-8')


def job(jobid):
    """Request the output of a job given its id."""
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


if __name__ == '__main__':
    _parser = argparse.ArgumentParser()
    _parser.add_argument('-f', action='store', dest='file',
                         type=str, help='the file to upload without full path')
    _parser.add_argument('-m', action='store', dest='descr',
                         type=str, help='description to upload')
    _parser.add_argument('-r', '--register', action='store_true',
                         dest='register', help='register vault list to ES')
    _parser.add_argument('-v', '--verbose', action='store_true',
                         dest='debug', help='verbose mode')
    _parser.add_argument('-d', '--delete', action='store',
                         dest='delete', type=str, help='delete an archive')
    _parser.add_argument('--jobs', action='store_true',
                         dest='jobs', help='retrieve job list')
    _parser.add_argument('-j', '--job', action='store',
                         dest='job', type=str, help='retrieve a job output')
    _parser.add_argument('-l', '--archive-list', action='store_true',
                         dest='list', help='retrieve the archive list')
    _parser.add_argument('-O', '--to-stdout', action='store_true',
                         dest='stdout', help='print output to stdout')

    _args = _parser.parse_args()

    if _args.stdout:
        output = sys.stdout

    if _args.debug:
        _logger.setLevel(logging.DEBUG)

    _main()
