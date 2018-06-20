import stat

BASE_DIR = "/grp/hst/cos2/cosmo"
CACHE = "/ifs/archive/ops/hst/public"
PERM_755 = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
PERM_872 = stat.S_ISVTX | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
CSUM_DIR = "tmp_out"
