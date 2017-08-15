import datetime
import luigi
import yaml
import requests
from common import url_is_valid, download

from pyquery import PyQuery as pq


class EsocialFetchAvailableSchemaPacks(luigi.Task):
    BASE_URL = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('esocial-urls.txt')

    def links(self):
        response = requests.get(self.BASE_URL, verify=False)
        d = pq(response.text)
        anchors = d("a:contains(XSD)")
        links = [l.attrib['href'] for l in anchors
                 if url_is_valid(l.attrib['href'])]
        assert links
        return links

    def run(self):
        with self.output().open('w') as f:
            f.writelines(link + '\n' for link in self.links())


class EsocialDownloadSchemaPacks(luigi.Task):
    def requires(self):
        return EsocialFetchAvailableSchemaPacks()

    def output(self):
        return luigi.LocalTarget('downloaded-esocial.yaml')

    def run(self):

        downloaded_files = []

        with self.input().open() as f:
            available_schema_packs = [i.strip() for i in f.readlines()]
        for sp in available_schema_packs:
            downloaded_file = download(sp, dest_dir='downloaded/esocial')
            downloaded_files.append(dict(
                url=sp,
                local_path=downloaded_file,
                utc_timestamp=datetime.datetime.utcnow().isoformat(),
            ))

        with self.output().open('w') as fo:
            fo.write(yaml.dump(downloaded_files, default_flow_style=False))
