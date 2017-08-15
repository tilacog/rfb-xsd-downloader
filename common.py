import pathlib
import requests
from urlib.parse import urlparse


def download(url, dest_dir='downloaded'):
    pathlib.Path(dest_dir).mkdir(exist_ok=True)
    r = requests.get(url, stream=True)
    local_filename = (pathlib.Path(dest_dir) /
                      (url.strip().split('/')[-1] + '.zip')).as_posix()
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return local_filename


def download_many(url_list):
    # TODO: make async
    downloaded = list(map(download, url_list))
    return downloaded


def url_is_valid(url):
    u = urlparse(url)
    if u.scheme and u.netloc and u.path:
        return True
