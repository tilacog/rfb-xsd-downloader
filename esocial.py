import luigi
import requests
from urllib.parse import urlparse

from pyquery import PyQuery as pq


class EsocialFetchAvailableSchemaPacks(luigi.Task):
    BASE_URL = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('esocial-urls.txt')

    def links(self):
        response = requests.get(self.BASE_URL, verify=False)
        d = pq(response.text)
        anchors = d("a:contains(XSD)")
        links = [l.attrib['href'] for l in anchors
                 if url_is_valid(l.attrib['href'])]
        assert links
        return links

    def run(self):
        with self.output().open('w') as f:
            f.writelines(link + '\n' for link in self.links())


def url_is_valid(url):
    u = urlparse(url)
    if u.scheme and u.netloc and u.path:
        return True
    return False
