# backup-ninja
Fast backup files to archive with encryption, deduplication, and compression.

Author: Peter Walhagen, 2023

Needs a config file (default "backup_config.json") with the following content:
```
{
    "table_dir": "<path-to-where-you-want-to-put-the-backup-filesystem-tables>",
    "file_dir": "<path-to-where-you-want-to-put-the-encrypted-backup-files>",
    "key_path": "<path-to-encryption-key>",
    "hard_remove": <bool>,
    "restore_dir": "<path-to-restore-dir>",
    "backup_roots": [
        "<src-path-to-backup-1>",
        "<src-path-to-backup-2>",
        ...
    ]
}
```
The key file shall contain a 32 bit key. You can generate it with e.g. 
```
from cryptography.fernet import Fernet
key = Fernet.generate_key()
with open(key_path, 'wb') as f:
    f.write(key)
```
Keep this key safe. If you loose it you can't restore the backup.
(Don't keep the file together with the backup archive, or you loose the purpose of the encryption)

As the backup archive is encrypted locally (and the key is kept separate from the archive) the files can be uploaded to cloud backup without any easy way of breaking in to the files.


The backup will make an archive entry for each file checksum. If `hard_remove` is set to false, outdated file entries will be put in a history table to be possible to be restored by finding the path from the associated logs. If a file is modified the checksum is changed and this can lead to a huge history if backing up often. If `hard_remove` is set to false some pruning every now and then will probably be needed (action `cleanup_soft`, `cleanup_hard`)

When restoring - all data is decrypted and decompressed and put under `restore_path` with their actual restore path appended while replacing the ":" in the drive with "_".


There is code for a soft backup which can run a lot faster and that compares file path, modification time, file id and file size instead of calculating the checksum to see if a file is already in the backup. Files that are to be added will always have their checksum calculated. This soft backup is not tested well and may not be necessary as the full backup runs quite fast anyway.

The io intensive tasks run on cpu_count // 2 processes (as many processors have hyperthreading which might not be easily utilized) and 4 threads per processor. This has empirically been a sweet spot for setting all cores to work while parallellising io actions.

run `python backup.py -h` for help with running the script.


