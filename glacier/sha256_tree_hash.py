#!/usr/bin/env python

import hashlib
import sys
import codecs

def get_chunks_sha256_hashes(filename, block_size=1048576):
    chunks = []
    
    with open(filename, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            sha256 = hashlib.sha256()
            sha256.update(block)
            chunks.append(sha256.digest())
            
    return chunks

def compute_sha256_tree_hash(chunks):
    prev_lvl_hashes = chunks

    while len(prev_lvl_hashes) > 1:
        curr_lvl_hashes = []
        for i in range(0, len(prev_lvl_hashes), 2):
            if len(prev_lvl_hashes) - i > 1:
                sha256 = hashlib.sha256()
                sha256.update(prev_lvl_hashes[i])
                sha256.update(prev_lvl_hashes[i + 1])
                curr_lvl_hashes.append(sha256.digest())
            else:
                curr_lvl_hashes.append(prev_lvl_hashes[i])

        prev_lvl_hashes = curr_lvl_hashes
        
    return codecs.getencoder('hex')(prev_lvl_hashes[0])[0].decode('UTF-8')

def _main():
    for f in sys.argv[1:]:
        chunks = get_chunks_sha256_hashes(f)
        checksum = compute_sha256_tree_hash(chunks)
        print(checksum)

if __name__ == '__main__':
    _main()