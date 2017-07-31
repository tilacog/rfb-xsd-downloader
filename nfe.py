import pathlib
import zipfile
from datetime import datetime
from functools import reduce

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

    def requires(self):
        return FetchAvailableSchemaPacks()

    def output(self):
        return luigi.LocalTarget('downloaded')

    def run(self):
        # get list of urls to download
        with self.input().open() as f:
            available_schema_packs = f.readlines()
        # download them all
        # TODO: be idempotent (only download missing files)
        download_many(available_schema_packs)


class FilterSchemaPacks(luigi.Task):
    '''
    Filter schema packs to be used.
    Will create symlinks for selected packs.
    '''
    def requires(self):
        return DownloadSchemaPacks()

    def output(self):
        return luigi.LocalTarget('selected')

    def run(self):

        for schema_info in self.filter_schemas().values():
            # make dir, if must
            dest_dir = pathlib.Path(self.output().path).resolve()
            dest_dir.mkdir(exist_ok=True)

            # put a symlink to downoaded schema-pack file
            target = pathlib.Path(schema_info['path']).resolve()
            assert target.exists()

            link = ((dest_dir / pathlib.Path(schema_info['schema-pack-name']))
                    .resolve())

            link.symlink_to(target)
            link.touch()

    def filter_schemas(self):
        'returns schema-packs of interest'
        directory = pathlib.Path(self.input().path)

        # builds a list of zipped files that contain inner files of interest
        schema_packs = filter(
            self.filter_criteria,
            map(zipfile.ZipFile,
                map(lambda x: x.as_posix(), directory.glob('*.zip')))
        )

        # helper function
        def _filter(selected, zipped_file):
            metadata = get_zip_metadata(zipped_file)
            key = metadata['schema-pack-name']
            if key not in selected:
                selected[key] = metadata
            else:  # keep only the latest
                present = selected[key]
                if metadata['last-modified'] > present['last-modified']:
                    selected[key] = metadata
            return selected

        return reduce(_filter, schema_packs, {})

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


def get_zip_metadata(zip_file):
    return {
        'schema-pack-name': zip_file.namelist()[0].split('/')[0],  # HACK
        'last-modified': datetime(*max(zip_file.infolist(),
                                       key=lambda x: x.date_time)
                                  .date_time),
        'path': zip_file.filename,
    }


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
    # TODO: make async
    downloaded = list(map(download, url_list))
    return downloaded
