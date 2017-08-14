import pathlib
import zipfile
from datetime import datetime
from functools import reduce

import luigi
import psycopg2
import requests
from psycopg2.extensions import quote_ident
from psycopg2.extras import Json
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


class UpsertDatabase(luigi.Task):
    'upserts schema packs into database'
    DB_HOST = luigi.Parameter()
    DB_USER = luigi.Parameter()
    DB_PSSWD = luigi.Parameter()
    DB_PORT = luigi.Parameter()
    DB_NAME = luigi.Parameter()
    DB_TABLE = luigi.Parameter()

    def requires(self):
        return FilterSchemaPacks()

    def iter_input(self):
        directory = pathlib.Path(self.input().path)
        symlinks = [x for x in directory.iterdir() if x.is_symlink()]
        assert symlinks
        for symlink in symlinks:
            yield symlink

    def build_records(self):
        for symlink in self.iter_input():
            zipped_schema_path = symlink.resolve().as_posix()

            # record fields
            document_type = 'nfe'  # TODO: remove this hardcoded reference
            zipped_data = open(zipped_schema_path, 'rb').read()
            metadata = get_zip_metadata(zipfile.ZipFile(zipped_schema_path))
            leading_schema = metadata.pop('leading_schema')
            version = metadata.pop('schema-pack-name')

            yield (document_type, version, zipped_data, leading_schema,
                   Json(metadata))

    def connection(self):
        conn = psycopg2.connect(host=self.DB_HOST, port=self.DB_PORT,
                                dbname=self.DB_NAME, user=self.DB_USER,
                                password=self.DB_PSSWD)
        conn.set_session(autocommit=False)
        return conn

    def run(self):
        records = list(self.build_records())
        conn = self.connection()
        cursor = conn.cursor()
        table_name = quote_ident(self.DB_TABLE, scope=conn)

        for record in records:
            try:
                cursor.execute("BEGIN")
                cursor.execute(f"""
                INSERT INTO {table_name}
                (document_type, version, zipped_data, leading_schema,
                metadata) VALUES (%s, %s, %s, %s, %s);
                """, record)
            except (psycopg2.IntegrityError):
                cursor.execute("ROLLBACK")
            else:
                cursor.execute("COMMIT")
        conn.close()


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


def get_zip_metadata(zip_file):
    metadata = {
        'schema-pack-name': zip_file.namelist()[0].split('/')[0],  # HACK
        'last-modified': datetime(*max(zip_file.infolist(),
                                       key=lambda x: x.date_time)
                                  .date_time).isoformat(),
        'path': pathlib.Path(zip_file.filename).name,
        'contents': [x.filename for x in zip_file.infolist()]
    }
    metadata['leading_schema'] = next(x for x in metadata['contents']
                                      if '/nfe' in x)
    return metadata
