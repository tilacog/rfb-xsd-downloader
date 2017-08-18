# rfb-xsd-downloader

Luigi pipeline to download and persist XSD files into a PostgreSQL database for the following [SPED](http://sped.rfb.gov.br/ "Portal do Sped") projects/document types:

- Nfe
- eSocial
- EFD-Reinf

## Running

Each document type has tasks for downloading, filtering and persisting XSD files. Use them directly if you don't need all the downstream tasks.

Don't forget to edit the `luigi.cfg.template` with your configs. For luigi to detect it automatically, just rename it as `luigi.cfg`.

### Downloading, filtering and upserting XSD content into a PostgreSQL database

```
$ luigi --module pipeline SyncDatabase
```

### Unloading downloaded XSD files into a target directory

```
$ luigi --module pipeline PrepareWorkspace
```

## TO-DO
1. Configuration is duplicated for each document type.
2. Each document type has its own series of classes (`luigi.Task`), but I think it would be nice if there was only one pipeline that could handle them as parameters.
3. `EFD-Reinf` has only two versions/releases and their URL's are hardcoded in `efdreinf.py` file.
