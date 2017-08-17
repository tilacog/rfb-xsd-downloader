import datetime
import pathlib
import re
import zipfile

import luigi
import requests
import yaml
from pyquery import PyQuery as pq

from common import download, url_is_valid


class EsocialFetchAvailableSchemaPacks(luigi.Task):
    BASE_URL = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('urls-esocial.txt')

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
            downloaded_files.append({
                'url': sp,
                'local-path': downloaded_file,
                'download-timestamp-utc':
                datetime.datetime.utcnow().isoformat(),
            })

        with self.output().open('w') as fo:
            fo.write(yaml.dump(downloaded_files, default_flow_style=False))


class EsocialFilterSchemaPacks(luigi.Task):
    def requires(self):
        return EsocialDownloadSchemaPacks()

    def output(self):
        return luigi.LocalTarget('selected-esocial.yaml')

    def run(self):
        filtered_files = []
        with self.input().open() as f:
            downloaded_schema_packs = yaml.load(f)
        for sp in downloaded_schema_packs:
            local_path = pathlib.Path(sp['local-path'])
            target_dir = pathlib.Path('selected/esocial/')
            target_dir.mkdir(exist_ok=True, parents=True)
            target_path = target_dir/local_path.name
            rezip_xsd_files(source_path=local_path.resolve().as_posix(),
                            target_path=target_path.resolve().as_posix())
            new_zip = zipfile.ZipFile(target_path.as_posix())

            metadata = {
                **sp,
                'schema-pack-name': pack_name(target_path.name),
                'contents': [i.filename for i in new_zip.infolist()],
                'last-modified': datetime.datetime(*max(
                    new_zip.infolist(),
                    key=lambda x: x.date_time
                ) .date_time).isoformat(),
                'leading-schema': None,
            }
            filtered_files.append(metadata)

        with self.output().open('w') as fo:
            fo.write(yaml.dump(filtered_files, default_flow_style=False))


def rezip_xsd_files(source_path, target_path):
    source = zipfile.ZipFile(source_path)
    target = zipfile.ZipFile(target_path, mode='w',
                             compression=zipfile.ZIP_STORED)

    xsd_files = [i for i in source.infolist()
                 if i.filename.lower().endswith('.xsd')]

    for xsd in xsd_files:
        target.writestr(xsd, source.read(xsd))


def pack_name(filename):
    pattern = r'\d+'
    numbers = [int(number) for number in re.findall(pattern, filename)]
    return 'v' + '-'.join(map(str, numbers))
