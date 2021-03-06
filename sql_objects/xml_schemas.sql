create table if not exists public.xml_schemas (
    id serial primary key,
    document_type text not null,
    version text not null,
    zipped_data bytea not null,
    metadata jsonb not null,
    created_at timestamp default now(),
    unique (document_type, version)
);

comment on table public.xml_schemas is 'Stores zipped XSD file packs';
comment on column public.xml_schemas.id is 'unique id for this record';
comment on column public.xml_schemas.document_type is 'the document type namecode';
comment on column public.xml_schemas.version is 'the version of the XSD pack release';
comment on column public.xml_schemas.zipped_data is 'a blob for zipped XSD files';
comment on column public.xml_schemas.metadata is 'metadata for the zipped blob';
comment on column public.xml_schemas.created_at is 'timestamp for this record';
