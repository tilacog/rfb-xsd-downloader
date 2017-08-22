FROM python:3.6.2-alpine

ENV NFE_BASE_URL "http://www.nfe.fazenda.gov.br/portal/listaConteudo.aspx?tipoConteudo=/fwLvLUSmU8="
ENV NFE_BASE_DOWNLOAD_URL "http://www.nfe.fazenda.gov.br/portal/"
ENV ESOCIAL_BASE_URL "https://portal.esocial.gov.br/institucional/documentacao-tecnica"

RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh postgresql-dev gcc musl-dev libxml2-dev libxslt-dev

WORKDIR /app/pipeline/

RUN git clone https://github.com/tilacog/rfb-xsd-downloader .

RUN pip install --no-cache-dir -r requirements.txt
