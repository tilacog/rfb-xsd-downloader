import pathlib
import luigi
import requests
from luigi.util import requires
from pyquery import PyQuery as pq

# TODO: put this hardcoded URL into a config context
BASE_URL = ("http://www.nfe.fazenda.gov.br/portal/listaConteudo.aspx"
            "?tipoConteudo=/fwLvLUSmU8=")
BASE_DOWNLOAD_URL = "http://www.nfe.fazenda.gov.br/portal/"


class FetchAvailableSchemaPacks(luigi.Task):
    URL = luigi.Parameter(default=BASE_URL)

    def output(self):
        return luigi.LocalTarget('urls.txt')

    def run(self):
        with self.output().open('w') as f:
            f.writelines(link + '\n' for link in self.links(self.URL))

    def links(self, url):
        d = pq(url=url)
        section = d('p:contains(OFICIAIS).tituloSessao + div')
        download_links = [link.attrib['href'].strip()
                          for link in section.children('p a')]
        return [f'{BASE_DOWNLOAD_URL}/{link}' for link in download_links]


@requires(FetchAvailableSchemaPacks)
class DownloadSchemaPacks(luigi.Task):
    'download all schema packs'
    def run(self):
        with self.input().open() as f:
            available_schema_packs = f.readlines()
        # if exists, skip
        # else, download


class UpsertDatabase(luigi.Task):
    'upserts schema packs into database'
    pass


def download(url, dest_dir='downloaded'):
    pathlib.Path(dest_dir).mkdir(exist_ok=True)
    r = requests.get(url, stream=True)
    local_filename = (pathlib.Path(dest_dir) / (url.split('/')[-1] + '.zip')).as_posix()
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return local_filename


def download_many(url_list):
    downloaded = list(map(download, url_list))
    return downloaded

with open('urls.txt') as f:
    data = f.read().splitlines()
download_many(data)
