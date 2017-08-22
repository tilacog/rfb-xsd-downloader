import datetime
import os
import zipfile

import luigi
import yaml
from pyquery import PyQuery as pq

from common import UpsertDatabase, download

# grab config from environment variables
ENVS = ('NFE_BASE_URL', 'NFE_BASE_DOWNLOAD_URL')
CONFIG = {var: os.environ[var] for var in ENVS}


class FetchAvailableSchemaPacks(luigi.Task):
    def output(self):
        return luigi.LocalTarget('urls-nfe.txt')

    def run(self):
        with self.output().open('w') as f:
            f.writelines(link + '\n'
                         for link in self.links(CONFIG['NFE_BASE_URL']))

    def links(self, url):
        d = pq(url=url)
        section = d('p:contains(OFICIAIS).tituloSessao + div')
        download_links = [link.attrib['href'].strip()
                          for link in section.children('p a')]
        return [f'{CONFIG["NFE_BASE_DOWNLOAD_URL"]}/{link}'
                for link in download_links]


class DownloadSchemaPacks(luigi.Task):
    'download all schema packs'

    def requires(self):
        return FetchAvailableSchemaPacks()

    def output(self):
        return luigi.LocalTarget('downloaded-nfe.yaml')

    def run(self):

        downloaded_files = []

        with self.input().open() as f:
            available_schema_packs = [i.strip() for i in f.readlines()]
        for sp in available_schema_packs:
            downloaded_file = download(sp, dest_dir='downloaded/nfe')
            downloaded_files.append({
                'url': sp,
                'local-path': downloaded_file,
                'download-timestamp-utc': (datetime.datetime.utcnow()
                                           .isoformat()),
            })

        with self.output().open('w') as fo:
            fo.write(yaml.dump(downloaded_files, default_flow_style=False))


class FilterSchemaPacks(luigi.Task):
    '''
    Filter schema packs to be used.
    Will create symlinks for selected packs.
    '''
    def requires(self):
        return DownloadSchemaPacks()

    def output(self):
        return luigi.LocalTarget('selected-nfe.yaml')

    def iter_input(self):
        with self.input().open() as f:
            return yaml.load(f)

    def run(self):
        selected = {}
        downloaded_files = self.iter_input()

        for df in downloaded_files:
            zipped = zipfile.ZipFile(df['local-path'])
            if not self.filter_criteria(zipped):
                continue
            metadata = get_zip_metadata(zipped)
            key = metadata['schema-pack-name']
            if key not in selected:
                df.update(metadata)
                selected[key] = df
            else:
                existing_ts = selected[key]['last-modified']
                incoming_ts = metadata['last-modified']
                if incoming_ts > existing_ts:
                    df.update(metadata)
                    selected[key] = df

        with self.output().open('w') as fo:
            fo.write(yaml.dump(list(selected.values())))

    @staticmethod
    def filter_criteria(zipped_file):
        list_of_filenames = zipped_file.namelist()
        requirements = {
            '/leiauteNFe': False,
            '/nfe': False
        }

        for name in list_of_filenames:
            for requirement in requirements:
                if requirement in name:
                    requirements[requirement] = True  # toogle
        if all(requirements.values()):
            return True
        return False


class NfeUpsertDatabase(UpsertDatabase):
    document_type = 'nfe'

    def requires(self):
        return FilterSchemaPacks()


def get_zip_metadata(zip_file):
    metadata = {
        'schema-pack-name': zip_file.namelist()[0].split('/')[0],  # HACK
        'contents': [x.filename for x in zip_file.infolist()],
        'last-modified': datetime.datetime(*max(zip_file.infolist(),
                                                key=lambda x: x.date_time)
                                           .date_time).isoformat(),
    }
    metadata['leading-schema'] = next(x for x in metadata['contents']
                                      if '/nfe' in x)
    return metadata
