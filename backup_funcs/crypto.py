import struct
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import gzip
import os
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from functools import partial



def key_from_password_and_salt(password, salt):
    return base64.urlsafe_b64encode(PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=480000).derive(password))



class SrcDstPathPair():
    def __init__(self, src_path, dst_path):
        self.src_path = src_path
        self.dst_path = dst_path




class ConcurrentEncryptor():
    def __init__(self, n_procs=None, n_threads_per_proc=None, key=None):
        self.key = key
        self.n_procs = max(1, os.cpu_count() // 2 if n_procs is None else n_procs)
        self.n_threads_per_proc = max(1, 4 if n_threads_per_proc is None else n_threads_per_proc)
        
    def store_files(self, srcdst_path_pairs, key=None, chunk_size=16*1024*1024):
        key = key or self.key

        n_paths = len(srcdst_path_pairs)
        n_paths_per_proc = n_paths // self.n_procs
        _srcdst_path_pairs = [srcdst_path_pairs[i*n_paths_per_proc:(i+1)*n_paths_per_proc] for i in range(self.n_procs-1)]
        _srcdst_path_pairs.append(srcdst_path_pairs[(self.n_procs-1)*n_paths_per_proc:])


        _p_store_files = partial(_store_files, key=key, chunk_size=chunk_size, n_threads=self.n_threads_per_proc)
        with Pool(self.n_procs) as pool:
            pool.map(_p_store_files, _srcdst_path_pairs)

    def restore_files(self, srcdst_path_pairs, key=None):
        key = key or self.key

        n_paths = len(srcdst_path_pairs)
        n_paths_per_proc = n_paths // self.n_procs
        _srcdst_path_pairs = [srcdst_path_pairs[i*n_paths_per_proc:(i+1)*n_paths_per_proc] for i in range(self.n_procs-1)]
        _srcdst_path_pairs.append(srcdst_path_pairs[(self.n_procs-1)*n_paths_per_proc:])

        _p_restore_files = partial(_restore_files, key=key, n_threads=self.n_threads_per_proc)
        with Pool(self.n_procs) as pool:
            pool.map(_p_restore_files, _srcdst_path_pairs)


def _store_file(srcdst_path_pair, key, chunk_size=16*1024*1024):
    crypto = Crypto(key)
    crypto.store_file(srcdst_path_pair[0], srcdst_path_pair[1], chunk_size=chunk_size)

def _store_files(srcdst_path_pairs, key, chunk_size=16*1024*1024, n_threads=4):
    _p_store_file = partial(_store_file, key=key, chunk_size=chunk_size)
    with ThreadPool(n_threads) as pool:
        pool.map(_p_store_file, srcdst_path_pairs)

def _restore_file(srcdst_path_pair, key):
    crypto = Crypto(key)
    crypto.restore_file(srcdst_path_pair[0], srcdst_path_pair[1])

def _restore_files(srcdst_path_pairs, key, n_threads=4):
    _p_restore_file = partial(_restore_file, key=key)
    with ThreadPool(n_threads) as pool:
        pool.map(_p_restore_file, srcdst_path_pairs)





class Crypto:    
    def __init__(self, key):
        self.fernet = Fernet(key)

    def store_file(self, src_path, dst_path, chunk_size=16*1024*1024):
        with open(src_path, 'rb') as fin:
            with gzip.open(dst_path, mode='wb') as fout:
                self.encrypt(fin, fout, chunk_size)

    def restore_file(self, src_path, dst_path):
        with gzip.open(src_path, 'rb') as fin:
            with open(dst_path, 'wb') as fout:
                self.decrypt(fin, fout)

    def encrypt(self, in_file_obj, out_file_obj, chunk_size=16*1024*1024):
        # Assert that we can write the chunk size to the file
        # Each encrypted chunk may be larger than the read one, but not more that 4x
        # (each chunk size is defined by a 4 byte unsigned integer => MAX 2**32 = 4GB)
        if chunk_size > 2**30:
            raise ValueError("Chunck size to large")
        
        # check the size of the file to be able to report progress
        while True:
            chunk = in_file_obj.read(chunk_size)
            if not chunk:
                break
            chunk = self.fernet.encrypt(chunk)
            out_file_obj.write(struct.pack('<I', len(chunk)))  # little endian unsigned integer
            out_file_obj.write(chunk)
            if len(chunk) < chunk_size:
                break
            

    def decrypt(self, in_file_obj, out_file_obj):
        while True:
            chunk_size_bytes = in_file_obj.read(4)
            if not chunk_size_bytes:
                break
            chunkSize = struct.unpack('<I', chunk_size_bytes)[0]
            chunk = in_file_obj.read(chunkSize)
            dec = self.fernet.decrypt(chunk)
            out_file_obj.write(dec)
        
