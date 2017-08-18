import datetime
import zipfile

import luigi
import yaml

from common import (UpsertDatabase, download, zip_file_last_modified,
                    zip_file_list)

# TODO: Don't use hardcoded urls
SCHEMA_PACK_URLS = {
    "1.1.1": "http://sped.rfb.gov.br/arquivo/download/2271",
    "1.0": "http://sped.rfb.gov.br/arquivo/download/2275",
}


class EfdreinfDownloadSchemaPacks(luigi.Task):
    def output(self):
        return luigi.LocalTarget('downloaded-efd-reinf.yaml')

    def run(self):
        downloaded_files = []

        for version, url in SCHEMA_PACK_URLS.items():
            downloaded_file = download(url, dest_dir='downloaded/efd-reinf')
            downloaded_files.append({
                'url': url,
                'local-path': downloaded_file,
                'download-timestamp-utc':
                datetime.datetime.utcnow().isoformat(),
                'schema-pack-name': version,
            })

        with self.output().open('w') as fo:
            fo.write(yaml.dump(downloaded_files, default_flow_style=False))


class EfdreinfPrepareSchemaPacks(luigi.Task):
    def requires(self):
        return EfdreinfDownloadSchemaPacks()

    def output(self):
        return luigi.LocalTarget('selected-efd-reinf.yaml')

    def run(self):
        selected = []
        with self.input().open() as f:
            downloaded_schema_packs = yaml.load(f)
        for sp in downloaded_schema_packs:
            zipped = zipfile.ZipFile(sp['local-path'])
            metadata = {
                **sp,
                'contents': zip_file_list(zipped),
                'last-modified': zip_file_last_modified(zipped),
                'leading-schema': None,
            }
            selected.append(metadata)

        with self.output().open('w') as fo:
            fo.write(yaml.dump(selected, default_flow_style=False))


class EfdreinfUpsertDatabase(UpsertDatabase):
    document_type = 'efd-reinf'

    def requires(self):
        return EfdreinfPrepareSchemaPacks()
