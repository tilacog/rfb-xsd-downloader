import os
import io
import pathlib
import zipfile

import luigi
import psycopg2
from psycopg2.extensions import quote_ident

from efdreinf import EfdreinfUpsertDatabase
from esocial import EsocialUpsertDatabase
from nfe import NfeUpsertDatabase


class SyncDatabase(luigi.WrapperTask):
    def requires(self):
        yield NfeUpsertDatabase()
        yield EsocialUpsertDatabase()
        yield EfdreinfUpsertDatabase()


class PrepareWorkspace(luigi.Task):
    target_directory = os.environ['XML_WORKSPACE_DIRECTORY']
    DATABASE_HOST = os.environ["DATABASE_HOST"]
    DATABASE_USER = os.environ["DATABASE_USER"]
    DATABASE_PASSWD = os.environ["DATABASE_PASSWD"]
    DATABASE_PORT = os.environ["DATABASE_PORT"]
    DATABASE_DB = os.environ["DATABASE_DB"]
    DATABASE_XSD_TABLE = os.environ["DATABASE_XSD_TABLE"]

    def run(self):
        target_dir = pathlib.Path(self.target_directory)
        if target_dir.exists():
            self.wipe(target_dir)
        target_dir.mkdir(exist_ok=True, parents=True)

        schema_packs = self.get_schema_packs()
        for pack_info in schema_packs:
            self.unpack(pack_info, at=target_dir)

    def unpack(self, pack_info, at):
        document_type, version, zipped_file_as_bytes = pack_info
        target_directory = at / document_type / version
        zipped_file = zipfile.ZipFile(io.BytesIO(zipped_file_as_bytes))
        zipped_file.extractall(path=target_directory)

    def get_schema_packs(self):
        conn = psycopg2.connect(host=self.DATABASE_HOST,
                                port=self.DATABASE_PORT,
                                dbname=self.DATABASE_DB,
                                user=self.DATABASE_USER,
                                password=self.DATABASE_PASSWD)
        conn.set_session(readonly=True)

        table_name = quote_ident(self.DATABASE_XSD_TABLE, scope=conn)
        cursor = conn.cursor()
        cursor.execute(f"""select document_type, version, zipped_Data
                           from {table_name};""")

        for record in cursor:
            yield record

        conn.close()

    def wipe(self, path):
        for sub in path.iterdir():
            if sub.is_dir():
                self.wipe(sub)
            else:
                path.unlink()
        path.rmdir()
