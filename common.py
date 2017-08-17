import pathlib
from urllib.parse import urlparse

import luigi
import psycopg2
import requests
import yaml
from psycopg2.extensions import quote_ident
from psycopg2.extras import Json


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


class UpsertDatabase(luigi.Task):
    'upserts schema packs into database'
    DB_HOST = luigi.Parameter()
    DB_USER = luigi.Parameter()
    DB_PSSWD = luigi.Parameter()
    DB_PORT = luigi.Parameter()
    DB_NAME = luigi.Parameter()
    DB_TABLE = luigi.Parameter()

    def iter_input(self):
        with self.input().open() as f:
            return yaml.load(f)

    def build_records(self):
        for selected_file in self.iter_input():
            # record fields
            document_type = self.document_type
            zipped_data = open(selected_file.pop('local-path'), 'rb').read()
            leading_schema = selected_file['leading-schema']
            version = selected_file['schema-pack-name']
            metadata = {
                'url': selected_file['url'],
                'last-modified': selected_file['last-modified'],
                'contents': selected_file['contents'],
                'download-timestamp-utc':
                selected_file['download-timestamp-utc'],
            }
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
