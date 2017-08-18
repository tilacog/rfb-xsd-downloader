import luigi
from nfe import NfeUpsertDatabase, FilterSchemaPacks
from esocial import EsocialUpsertDatabase, EsocialFilterSchemaPacks
from efdreinf import EfdreinfUpsertDatabase, EfdreinfPrepareSchemaPacks


class SyncDatabase(luigi.WrapperTask):
    def requires(self):
        yield NfeUpsertDatabase()
        yield EsocialUpsertDatabase()
        yield EfdreinfUpsertDatabase()


class PrepareWorkspace(luigi.WrapperTask):
    def requires(self):
        yield FilterSchemaPacks()
        yield EsocialFilterSchemaPacks()
        yield EfdreinfPrepareSchemaPacks()
