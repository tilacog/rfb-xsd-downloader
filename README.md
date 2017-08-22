# rfb-xsd-downloader

Luigi pipeline to download and persist XSD files into a PostgreSQL database for the following [SPED](http://sped.rfb.gov.br/ "Portal do Sped") projects/document types:

- Nfe
- eSocial
- EFD-Reinf

## Running

Each document type has tasks for downloading, filtering and persisting XSD files. Use them directly if you don't need all the downstream tasks.

This pipeline uses environment variables instead of a luigi configuration file. Check the `.env.template` file for a list of required environment variables to be set.

### Downloading, filtering and upserting XSD content into a PostgreSQL database

```
$ luigi --module pipeline SyncDatabase
```

### Unloading downloaded XSD files into a target directory

```
$ luigi --module pipeline PrepareWorkspace
```

## TO-DO
2. Each document type has its own series of classes (`luigi.Task`), but I think it would be nice if there was only one pipeline that could handle them as parameters.
3. `EFD-Reinf` has only two versions/releases and their URL's are hardcoded in `efdreinf.py` file.
