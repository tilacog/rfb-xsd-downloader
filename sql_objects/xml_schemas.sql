create table public.xml_schemas (
    id serial primary key,
    document_type text not null,
    version text not null,
    zipped_data bytea not null,
    leading_schema text,
    metadata jsonb not null,
    created_at timestamp default now(),
    unique (document_type, version),
    check ((metadata->'contents')::jsonb ? leading_schema or leading_schema is null)
);

comment on table public.xml_schemas is 'Stores zipped XSD file packs';
comment on column public.xml_schemas.id is 'unique id for this record';
comment on column public.xml_schemas.document_type is 'the document type namecode';
comment on column public.xml_schemas.version is 'the version of the XSD pack release';
comment on column public.xml_schemas.zipped_data is 'a blob for a zipped XSD files';
comment on column public.xml_schemas.leading_schema is 'the schema to be called for XSD validation. (Nulls means multiple candiates for validation lead)';
comment on column public.xml_schemas.metadata is 'metadata for the zipped blob';
comment on column public.xml_schemas.created_at is 'timestamp for this record';
