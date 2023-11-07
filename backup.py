from backup_funcs.archive import Archive
from backup_funcs.misc import pretty_size
from pathlib import Path
import argparse
import time

import logging

logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


# Main function
def run_backup(backup_roots, table_dir, file_dir, key_dir):
    
    with open(key_dir, 'rb') as f:
        key = f.read()
    archiver = Archive(table_dir, file_dir, key)
    key = None


    archiver.backup(backup_roots, full=True, hard_remove=False)
    
    #archiver.restore('D:/tmp/_restore')
    #archiver.cleanup_keep_latest_per_path_each_year()
    #archiver.cleanup_delete_all_history()

# Call main function
if __name__ == "__main__":
    
    # parse command line arguments
    #src_dir = Path('C:/Users/Peter/OneDrive - Walhagen Engineering AB/BACKUP-KAMERA/')
    backup_roots = [Path('D:/tmp')]
    #src_dir = Path('C:/Users/Peter/OneDrive/Documents')


    table_dir = Path('C:/Users/Peter/OneDrive - Walhagen Engineering AB/_BACKUP/')
    file_dir = Path('C:/Users/Peter/OneDrive - Walhagen Engineering AB/_BACKUP/files')
    key_dir = Path('~/.backup/.key').expanduser()


    run_backup(backup_roots, table_dir, file_dir, key_dir)




