import datetime
import os
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


def zip_file_list(zipped_file):
    return [i.filename for i in zipped_file.infolist()]


def zip_file_last_modified(zipped_file):
    return datetime.datetime(
        *max(zipped_file.infolist(), key=lambda x: x.date_time).date_time
    ).isoformat()


class UpsertDatabase(luigi.Task):
    'upserts schema packs into database'
    DATABASE_HOST = os.environ["DATABASE_HOST"]
    DATABASE_USER = os.environ["DATABASE_USER"]
    DATABASE_PASSWD = os.environ["DATABASE_PASSWD"]
    DATABASE_PORT = os.environ["DATABASE_PORT"]
    DATABASE_DB = os.environ["DATABASE_DB"]
    DATABASE_XSD_TABLE = os.environ["DATABASE_XSD_TABLE"]

    def iter_input(self):
        with self.input().open() as f:
            return yaml.load(f)

    def build_records(self):
        for selected_file in self.iter_input():
            # record fields
            document_type = self.document_type
            zipped_data = open(selected_file.pop('local-path'), 'rb').read()
            version = selected_file['schema-pack-name']
            metadata = {
                'url': selected_file['url'],
                'last-modified': selected_file['last-modified'],
                'contents': selected_file['contents'],
                'download-timestamp-utc':
                selected_file['download-timestamp-utc'],
            }
            yield (document_type, version, zipped_data, Json(metadata))

    def connection(self):
        conn = psycopg2.connect(host=self.DATABASE_HOST,
                                port=self.DATABASE_PORT,
                                dbname=self.DATABASE_DB,
                                user=self.DATABASE_USER,
                                password=self.DATABASE_PASSWD)
        conn.set_session(autocommit=False)
        return conn

    def run(self):
        records = list(self.build_records())
        conn = self.connection()
        cursor = conn.cursor()
        table_name = quote_ident(self.DATABASE_XSD_TABLE, scope=conn)

        for record in records:
            try:
                cursor.execute("BEGIN")
                cursor.execute(f"""
                INSERT INTO {table_name}
                (document_type, version, zipped_data, metadata)
                VALUES (%s, %s, %s, %s);
                """, record)
            except (psycopg2.IntegrityError):
                cursor.execute("ROLLBACK")
            else:
                cursor.execute("COMMIT")
        conn.close()

        pathlib.Path(self.output().path).touch()  # indicates task completion

    def output(self):
        return luigi.LocalTarget(f'upserted-{self.document_type}.timestamp')
