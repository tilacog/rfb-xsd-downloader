import pathlib
import requests
from urllib.parse import urlparse


def download(url, dest_dir='downloaded'):
    pathlib.Path(dest_dir).mkdir(exist_ok=True, parents=True)
    r = requests.get(url, stream=True, verify=False)

    local_filename = (pathlib.Path(dest_dir) /
                      (url.strip().split('/')[-1])).as_posix()
    if not local_filename.endswith('.zip'):
        local_filename += '.zip'
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
