import redis
import os
from datetime import datetime, timedelta
import shutil


def basic_config(**kwargs):
    for k, v in kwargs.values():
        if not hasattr(Config, k):
            pass
        setattr(Config, k, v)


class Config(object):
    redis_host = "localhost"
    redis_port = 6379
    redis_db = 0
    redis_expire = 1800

    disk_path = "./"
    disk_expire = 7


class LayerCache(object):
    def __init__(self):
        self._rcache = RedisCache()
        self._dcache = DiskCache()

    def set(self, k, v):
        self._rcache.set(k, v)
        self._dcache.set(k, v)

    def get(self, k, date):
        v = self._rcache.get(k)
        if v:
            return v
        return self._dcache.get(k, date)

    def clean(self):
        self._dcache.clean()


class RedisCache(object):
    def __init__(self):
        self.expire = Config.redis_expire
        self.redis = redis.StrictRedis(host=Config.redis_host, port=Config.redis_port, db=Config.redis_db)
        self.redis.ping()

    def set(self, k, v):
        return self.redis.setex(k, self.expire, v)

    def get(self, k):
        return self.redis.get(k)


class DiskCache(object):
    subdir_fmt = "%Y%m%d"

    def __init__(self):
        if not os.path.exists(Config.disk_path):
            raise Exception("path {} not found".format(Config.disk_path))
        self.path = Config.disk_path
        self.expire = Config.disk_expire

    def set(self, k, v):
        subdir = "{}/{}".format(self.path, datetime.now().strftime(self.subdir_fmt))
        self._check_dir(subdir)

        file_path = "{}/{}".format(subdir, k)
        with open(file_path, "wb") as f:
            f.write(v)

    def get(self, k, date):
        if isinstance(date, datetime):
            date = date.strftime(self.subdir_fmt)
        elif isinstance(date, str):
            pass
        else:
            raise Exception("unsupported date type {}".format(type(date)))

        subdir = "{}/{}".format(self.path, date)
        if not os.path.exists(subdir):
            return None
        file_path = "{}/{}".format(subdir, k)
        if not os.path.isfile(file_path):
            return None
        with open(file_path, "rb") as f:
            content = f.read()
        return content

    def clean(self):
        files = os.listdir(self.path)
        limit = datetime.now() - timedelta(days=self.expire)

        for f in files:
            subdir = "{}/{}".format(self.path, f)
            if os.path.isfile(subdir):  # just ignore file
                continue
            dt = datetime.strptime(f, self.subdir_fmt)
            if dt >= limit:
                continue
            shutil.rmtree(subdir)

    @staticmethod
    def _check_dir(subdir):
        if not os.path.exists(subdir):
            os.mkdir(subdir)
