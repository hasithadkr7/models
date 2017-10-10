DEFAULT_WRF_HOME = '/mnt/disks/wrf-mod/'


## GFS configs
# DEFAULT_GFS_DATA_URL = 'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.YYYYMMDDCC/'
DEFAULT_GFS_DATA_URL = 'http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.YYYYMMDDCC/'
DEFAULT_GFS_DATA_INV = 'gfs.tCCz.pgrb2.RRRR.fFFF'
DEFAULT_THREAD_COUNT = 8
DEFAULT_RETRIES = 5
DEFAULT_DELAY_S = 60
DEFAULT_CYCLE = '00'
DEFAULT_RES = '0p50'
DEFAULT_PERIOD = 3
DEFAULT_STEP = 3
DEFAULT_START_INV = 0


DEFAULT_EM_REAL_PATH = 'WRFV3/test/em_real/'
DEFAULT_WPS_PATH = 'WPS/'
DEFAULT_PROCS = 4
DEFAULT_NAMELIST_INPUT_TEMPLATE = 'namelist.input'
DEFAULT_NAMELIST_WPS_TEMPLATE = 'namelist.wps'


LOGGING_ENV_VAR = 'LOG_YAML'
