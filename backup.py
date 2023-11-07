from backup_funcs.archive import Archive
from backup_funcs.misc import pretty_size
from pathlib import Path
import json
import os
import argparse





# Call main function
if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description='Fast backup files to archive with encryption, deduplication, and compression.')
    # config file by -c
    parser.add_argument('-c', '--config', type=str, default='backup_config.json', help='config file')

    # do backup by default, can also do restore and cleanup_soft, cleanup_hard
    # specify by --action -a
    parser.add_argument('-a', '--action', type=str, default='backup', help='action to perform', choices=['backup', 'restore', 'cleanup_soft', 'cleanup_hard', 'info'])


    args = parser.parse_args()
    config_file = Path(args.config)
    if not config_file.exists():
        raise ValueError('Config file does not exist')
    with open(config_file) as f:
        config = json.load(f)
    table_dir = Path(os.path.expandvars(config['table_dir'])).expanduser()
    file_dir = Path(os.path.expandvars(config['file_dir'])).expanduser()
    key_path = Path(os.path.expandvars(config['key_path'])).expanduser()
    restore_dir = Path(os.path.expandvars(config['restore_dir'])).expanduser()
    backup_roots = [Path(os.path.expandvars(root)).expanduser() for root in config['backup_roots']]
    hard_remove = config['hard_remove']

    with open(key_path, 'rb') as f:
        key = f.read()
    archiver = Archive(table_dir, file_dir, key)

    if args.action == 'backup':
        archiver.backup(backup_roots, full=True, hard_remove=hard_remove)
    elif args.action == 'restore':
        archiver.restore(restore_dir)
    elif args.action == 'cleanup_soft':
        print('Will prune history to keep at most 1 copy of each file per path per year. Continue? y/n')
        if input() == 'y':
            archiver.cleanup_keep_latest_per_path_each_year()
    elif args.action == 'cleanup_hard':
        print('Will delete historical archive file entries completely. Continue? y/n')
        if input() == 'y':
            archiver.cleanup_delete_all_history()
    elif args.action == 'info':
        archiver.info(True)
    




