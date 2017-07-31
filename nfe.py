import pathlib
import luigi
import requests
from pyquery import PyQuery as pq


class FetchAvailableSchemaPacks(luigi.Task):
    BASE_URL = luigi.Parameter()  # url of xsd listings page
    BASE_DOWNLOAD_URL = luigi.Parameter()  # base url of xsd files

    def output(self):
        return luigi.LocalTarget('urls.txt')

    def run(self):
        with self.output().open('w') as f:
            f.writelines(link + '\n' for link in self.links(self.BASE_URL))

    def links(self, url):
        d = pq(url=url)
        section = d('p:contains(OFICIAIS).tituloSessao + div')
        download_links = [link.attrib['href'].strip()
                          for link in section.children('p a')]
        return [f'{self.BASE_DOWNLOAD_URL}/{link}' for link in download_links]


class DownloadSchemaPacks(luigi.Task):
    'download all schema packs'

    DEST_DIR = luigi.Parameter()

    def requires(self):
        return FetchAvailableSchemaPacks()

    def output(self):
        return luigi.LocalTarget('downloaded.txt')

    def run(self):
        # get list of urls to download
        with self.input().open() as f:
            available_schema_packs = f.readlines()
        # download them all
        # TODO: be idempotent (only download missing files)
        downloaded = download_many(available_schema_packs)
        # write list of downloaded files to a file
        with self.output().open('w') as f2:
            f2.writelines(i + '\n' for i in downloaded)


class UpsertDatabase(luigi.Task):
    'upserts schema packs into database'
    pass


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
    downloaded = list(map(download, url_list))
    return downloaded
