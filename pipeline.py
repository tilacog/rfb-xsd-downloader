import luigi
from nfe import NfeUpsertDatabase
from esocial import EsocialUpsertDatabase
from efdreinf import EfdreinfUpsertDatabase


class Sync(luigi.WrapperTask):
    def requires(self):
        yield NfeUpsertDatabase()
        yield EsocialUpsertDatabase()
        yield EfdreinfUpsertDatabase()
