from pathlib import Path
import os 
import hashlib




class FileInfo():
    def __init__(self, path, calculate_checksum=False):
        self.path = Path(path)
        fstat = self.path.stat()
        
        self.mtime = fstat.st_mtime
        self.size = fstat.st_size
        self.ino = fstat.st_ino
        self.name = self.path.name
        self.ext = self.path.suffix
        
        self.checksum = file_checksum(self.path) if calculate_checksum else None
            

    def __repr__(self):
        return f'{self.name}, {self.ino}, {self.checksum}'
        


# ------------------------------------


def file_checksum(fpath, chunkSize=1024*1024) -> str:
    checksum = hashlib.sha256()
    with open(fpath, 'rb') as f:
        while True:
            data = f.read(chunkSize)
            if not data:
                break
            checksum.update(data)
    return checksum.hexdigest()


def file_checksum_partial(fpath, chunkSize=256*1024) -> str:
    checksum = hashlib.sha256()
    fsize = os.path.getsize(fpath)
    with open(fpath, 'rb') as f:
        # read one chunk each at beginning, middle and end of file
        if fsize <= 3*chunkSize:
            data = f.read()
            checksum.update(data)
            return checksum.digest()
        else:
            # chunk at beginning of file
            data = f.read(chunkSize)
            checksum.update(data)
            
            # chunk in middle of file
            f.seek((fsize - chunkSize)//2, os.SEEK_SET)
            data = f.read(chunkSize)
            checksum.update(data)

            # chunk at end of file
            f.seek(fsize - chunkSize, os.SEEK_SET)
            data = f.read(chunkSize)   
            checksum.update(data)                             
    return checksum.hexdigest()

