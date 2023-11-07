import struct
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import gzip


def key_from_password_and_salt(password, salt):
    return base64.urlsafe_b64encode(PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=480000).derive(password))


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
        
