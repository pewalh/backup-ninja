from pathlib import Path
from enum import Enum
import datetime as dt
import time
from typing import Optional
import os
import json
import shutil
from pydantic import BaseModel

from .file_info import FileInfo
from .crypto import ConcurrentEncryptor
from .scanner import Scanner
from .logger import Logger
from .misc import pretty_size, pretty_time

logger = Logger()


"""
Event of the current state of a file, i.e. regarding checksum. A modified file will create one ADDED and one REMOVED event since a new state appears.
"""
class BlobEvent(Enum):
    ADDED = 1 # added to path, can be part of creation, move or copy
    REMOVED = 2 # removed from path, can be part of move, modification or deletion
    

class ArchiveLogEvent(BaseModel):
    timestamp:str
    event:str
    path:Optional[str]

    @classmethod
    def from_event(cls, event:BlobEvent, path:Optional[str]=None):
        ts = dt.datetime.now().astimezone().isoformat()
        return cls(timestamp=ts, event=event.name, path=Path(path).as_posix() if path is not None else None)


class ArchiveFilePointer(BaseModel):
    path:str
    ino:int
    mtime:float
    size:int
    


class ArchiveEntry(BaseModel):
    checksum:str
    fptrs:list[ArchiveFilePointer]
    log:list[ArchiveLogEvent]
    arch_size:int

    @classmethod
    def from_checksum(cls, checksum): 
        return cls(checksum=checksum, fptrs=[], log=[], arch_size=0)
        





"""
Archive class for storing and restoring files and keeping track of the backup archive
"""
class Archive():
    def __init__(self, table_dir, file_dir, key):
        self.table_dir = Path(table_dir)
        self.file_dir = Path(file_dir)
        
        if not file_dir.exists():
            os.makedirs(file_dir)
        if not table_dir.exists():
            os.makedirs(table_dir)

        self.crypto = ConcurrentEncryptor(key=key)
        self.active = {} # checksum : ArchiveEntry
        self.active_ino = {} # ino : checksum - indexed for fast lookup
        self.history = {} # checksum : ArchiveEntry

        
        self._load_archive_table()


    def _load_archive_table(self):
        archive_path = self.table_dir / 'archive.json'

        if not archive_path.exists():
            return False
        
        with open(archive_path, 'r') as fin:
            archive = json.load(fin)
        
        if not ('active' in archive and 'history' in archive):
            return False
        
        _active = [ArchiveEntry(**entry) for entry in archive['active']]
        _history = [ArchiveEntry(**entry) for entry in archive['history']]
        self.active = {entry.checksum: entry for entry in _active}
        self.history = {entry.checksum: entry for entry in _history}
        self.active_ino = {fptr.ino: entry.checksum for entry in _active for fptr in entry.fptrs}
        return True
    

    def _store_archive_table(self):
        # checksum is in the entry so no need to store it twice
        f_table = {
            'active':  [entry.model_dump() for entry in self.active.values()],
            'history': [entry.model_dump() for entry in self.history.values()]
        }
        archive_path = self.table_dir / 'archive.json'
        backup_path = self.table_dir / 'archive.json.bak'
        failed_path = self.table_dir / 'archive.json.failed'

        # backup old archive table
        if archive_path.exists():
            shutil.copyfile(archive_path, backup_path)
        
        # serialize and store with json
        with open(archive_path, 'w') as fout:
            json.dump(f_table, fout)
        
        # validate that the files are stored correctly, i.e. try to load them again
        success = False
        try: 
            success = self._load_archive_table()
        except:
            success = False
        if not success:
            # restore backup
            if archive_path.exists():
                shutil.copyfile(archive_path, failed_path)
            if backup_path.exists():
                shutil.copyfile(backup_path, archive_path)
            raise ValueError('Could not store archive table correctly. Restoring backup.')
                

    def _archived_fpath(self, checksum):
        return self.file_dir / checksum[:2] / (checksum+'.enc')



    def _remove_file(self, checksum):
        arch_path = self._archived_fpath(checksum)
        if os.path.exists(arch_path):
            os.remove(arch_path)
    

    def _check_archive_file(self, checksum, expected_size):
        arch_path = self._archived_fpath(checksum)
        exists = arch_path.exists()
        if exists:
            if arch_path.stat().st_size == expected_size:
                return True
        return False
        


    def _update_archive(self, scanned_files_with_checksum:list[FileInfo], hard_remove=False):

        scanned_chck2finfo = {}
        for finfo in scanned_files_with_checksum:
            if finfo.checksum not in scanned_chck2finfo:
                scanned_chck2finfo[finfo.checksum] = [finfo]
            else:
                scanned_chck2finfo[finfo.checksum].append(finfo)

        n_removed = 0
        n_added = 0
        n_errs = 0
        n_path_change = 0

        # update existing table - remove and move
        actve_chksums = list(self.active.keys())
        for checksum in actve_chksums:
            entry = self.active[checksum]
            if entry.checksum not in scanned_chck2finfo:
                
                # add file to history and remove from active and active_ino
                for fptr in entry.fptrs:
                    entry.log.append(ArchiveLogEvent.from_event(BlobEvent.REMOVED, fptr.path))
                entry.fptrs = []
                self.active.pop(entry.checksum)
                self.history[entry.checksum] = entry
                for fptr in entry.fptrs:
                    self.active_ino.pop(fptr.ino)
                if hard_remove:
                    self._remove_file(checksum)
                n_removed += 1
            else:
                finfos = scanned_chck2finfo[entry.checksum]
                # check if we have moved or copied the file
                arch_paths = [os.path.normpath(fptr.path) for fptr in entry.fptrs]
                curr_paths = [os.path.normpath(finfo.path) for finfo in finfos]

                # log any changes
                new_paths = [cpath for cpath in curr_paths if cpath not in arch_paths]
                old_paths = [apath for apath in arch_paths if apath not in curr_paths]
                
                if len(new_paths) == 0 and len(old_paths) == 0:
                    # no change
                    continue
                
                n_path_change += 1

                for path in new_paths:
                    entry.log.append(ArchiveLogEvent.from_event(BlobEvent.ADDED, path))
                for path in old_paths:
                    entry.log.append(ArchiveLogEvent.from_event(BlobEvent.REMOVED, path))

                # update file pointers
                entry.fptrs = []
                for finfo in finfos:
                    entry.fptrs.append(ArchiveFilePointer(path=finfo.path.as_posix(), ino=finfo.ino, mtime=finfo.mtime, size=finfo.size))

                # ino index does not need to be updated since we have not changed the file

        # add new files
        files_to_store = {} # checksum : entry
        for checksum, finfos in scanned_chck2finfo.items():
            if checksum in self.active:
                if self._check_archive_file(checksum, self.active[checksum].arch_size):
                    continue
                else:
                    logger.warning(f'Archive file not exists or size mismatch from table: {checksum}')
                    n_errs += 1
            
            entry = ArchiveEntry.from_checksum(checksum)
            for finfo in finfos:
                entry.fptrs.append(ArchiveFilePointer(path=finfo.path.as_posix(), ino=finfo.ino, mtime=finfo.mtime, size=finfo.size))
                entry.log.append(ArchiveLogEvent.from_event(BlobEvent.ADDED, finfo.path))
            
            files_to_store[checksum] = entry

        # store new files
        logger.info(f'Storing {len(files_to_store)} new archive files...')

        # prepare folders and srcdst_path_pairs
        srcdst_path_pairs = []
        for checksum, entry in files_to_store.items():
            arch_path = self._archived_fpath(checksum)
            src_path = entry.fptrs[0].path
            srcdst_path_pairs.append((src_path, arch_path))
            if not arch_path.parent.exists():
                os.makedirs(arch_path.parent)

        # store files
        self.crypto.store_files(srcdst_path_pairs)
        
        # update archive table
        for checksum, entry in files_to_store.items():
            entry.arch_size = self._archived_fpath(checksum).stat().st_size
            self.active[checksum] = entry
            for finfo in finfos:
                self.active_ino[finfo.ino] = checksum
            n_added += 1
        
        logger.info(f'Added {n_added} files, removed {n_removed} files, changed path for {n_path_change} files, {n_errs} errors.')


        self._store_archive_table()


            
    def _get_checksum_from_meta(self, finfo:FileInfo):
        checksum = self.active_ino.get(finfo.ino, None)
        if checksum is not None:
            for loc in self.active[checksum].fptrs:
                if ((loc.path == finfo.path) and
                    (loc.ino == finfo.ino) and
                    (loc.mtime == finfo.mtime) and
                    (loc.size == finfo.size)):
                    return checksum
        return None
    

    def backup(self, src_dir, full=True, hard_remove=False):
        t0 = time.time()
        if full:
            logger.info('Scanning directory tree and calculating checksums... (might take a while)')
        else:
            logger.info('Scanning directory tree without calculating checksums...')

        scanner = Scanner()
        scanner.scan_directory_tree(src_dir, with_checksum=full)
        total_size = sum([finfo.size for finfo in scanner.files])
        logger.info(f'Scanned {len(scanner.files)} files with total size {pretty_size(total_size)}.')

        finfo_with_checksum = []
        if full:
            finfo_with_checksum = scanner.files
        else:
            # get checksums lazily first (if everything else compares)
            finfo_with_checksum = []
            paths_without_checksum = []
            for finfo in scanner.files:
                checksum = self._get_checksum_from_meta(finfo)
                if checksum is None:
                    paths_without_checksum.append(finfo.path)
                else:
                    finfo.checksum = checksum
                    finfo_with_checksum.append(finfo)

            # get checksums for files without checksum
            logger.info(f'Calculating checksum for {len(paths_without_checksum)} files...')
            finfo_with_checksum2 = scanner.create_file_entries(paths_without_checksum, with_checksum=True)
            finfo_with_checksum.extend(finfo_with_checksum2)


        logger.info('Updating archive...')
        self._update_archive(finfo_with_checksum, hard_remove=hard_remove)

        t = time.time() - t0
        logger.info(f'Done backup. Time spent: {pretty_time(t)}.')
        return self.info(log=True)


    def cleanup(self):
        # TODO remove old deleted files - keep some history
        pass


    def restore(self, restore_base_path):
        logger.info(f'Restoring all files into: {restore_base_path}')

        # prepare folders and srcdst_path_pairs
        srcdst_path_pairs = []
        for checksum, entry in self.active.items():
            arch_path = self._archived_fpath(checksum)
            for fptr in entry.fptrs:
                dst_path = Path(fptr.path)
                dst_path = Path(restore_base_path) / dst_path.absolute().as_posix().replace(':','_')
                srcdst_path_pairs.append((arch_path, dst_path))
                if not dst_path.parent.exists():
                    os.makedirs(dst_path.parent)
        
        logger.info(f'Restoring {len(srcdst_path_pairs)} files...')
        
        # restore files
        self.crypto.restore_files(srcdst_path_pairs)

        total_size = sum([fptr.size for entry in self.active.values() for fptr in entry.fptrs])
        logger.info(f'Restored {len(srcdst_path_pairs)} files, {pretty_size(total_size)}.')


    def info(self, log=False) -> dict:
        info = {}
        info['n_active'] = len(self.active)
        info['n_history'] = len(self.history)
        info['restore_size'] = sum([entry.fptrs[0].size for entry in self.active.values()])
        info['archive_size_active'] = sum([entry.arch_size for entry in self.active.values()])
        info['archive_size_history'] = sum([entry.arch_size for entry in self.history.values()])
        if log:
            logger.info(f"Archive-active: {info['n_active']} files, size for restore: {pretty_size(info['restore_size'])}. Size in archive: {pretty_size(info['archive_size_active'])}")
            logger.info(f"Archive-history: {info['n_history']} files. Size in archive: {pretty_size(info['archive_size_history'])}")
            
        return info
