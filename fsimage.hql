CREATE TABLE IF NOT EXISTS monitoring.fsimage
(
  Path string COMMENT 'Full path of the file or directory.',
  Replication int COMMENT 'Replication factor.',
  ModificationTime bigint COMMENT 'The date of modification.',
  AccessTime bigint COMMENT 'Date of last access.',
  PreferredBlockSize int COMMENT 'The size of the block used.',
  BlocksCount int COMMENT 'Number of blocks.',
  FileSize bigint COMMENT 'File size.',
  NSQUOTA bigint COMMENT 'Files+Dirs quota, -1 is disabled.',
  DSQUOTA bigint COMMENT 'Space quota, -1 is disabled.',
  Permission string COMMENT 'Permissions used, user, group (Unix permission).',
  UserName string COMMENT 'Owner.',
  GroupName string COMMENT 'Group.'
)
PARTITIONED BY (Parsed int)
STORED AS ORC;

