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
    target_directory = luigi.Parameter()
    DB_HOST = luigi.Parameter()
    DB_USER = luigi.Parameter()
    DB_PSSWD = luigi.Parameter()
    DB_PORT = luigi.Parameter()
    DB_NAME = luigi.Parameter()
    DB_TABLE = luigi.Parameter()

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
        conn = psycopg2.connect(host=self.DB_HOST, port=self.DB_PORT,
                                dbname=self.DB_NAME, user=self.DB_USER,
                                password=self.DB_PSSWD)
        conn.set_session(readonly=True)

        table_name = quote_ident(self.DB_TABLE, scope=conn)
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
