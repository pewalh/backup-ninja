import os
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from .file_info import FileInfo
from functools import partial



class Scanner():
    def __init__(self, n_procs=None, n_threads_per_proc=None):
        self.n_procs = os.cpu_count() // 2 if n_procs is None else n_procs
        self.n_threads_per_proc = 4 if n_threads_per_proc is None else n_threads_per_proc
        self.files = []


    def scan_directory_tree(self, root_path, with_checksum=True):
        fpaths = []
        for root,_,files in os.walk(root_path):
            fpaths.extend([os.path.join(root, file) for file in files])
        
        file_entries = self.create_file_entries(fpaths, with_checksum=with_checksum)
        self.files.extend(file_entries)


    def create_file_entries(self, fpaths, with_checksum=True):

        n_paths = len(fpaths)
        n_paths_per_proc = n_paths // self.n_procs
        jagged_fpaths = [fpaths[i*n_paths_per_proc:(i+1)*n_paths_per_proc] for i in range(self.n_procs-1)]
        jagged_fpaths.append(fpaths[(self.n_procs-1)*n_paths_per_proc:])
        
        
        _create_file_entries = partial(create_file_entries, n_threads=self.n_threads_per_proc, with_checksum=with_checksum)
        f_entries = []
        if self.n_procs <= 1:
            f_entries = map(_create_file_entries, jagged_fpaths)
        else:
            with Pool(self.n_procs) as pool:
                f_entries = pool.map(_create_file_entries, jagged_fpaths)
        # flatten list of lists
        f_entries = [item for sublist in f_entries for item in sublist]
        return f_entries
        


def create_file_entry(path, with_checksum=True):
    return FileInfo(path, calculate_checksum=with_checksum)

def create_file_entries(paths, n_threads=4, with_checksum=True):
    entries = []
    _create_file_entry = partial(create_file_entry, with_checksum=with_checksum)
    with ThreadPool(n_threads) as pool:
        entries.extend(pool.map(_create_file_entry, paths))
    return entries