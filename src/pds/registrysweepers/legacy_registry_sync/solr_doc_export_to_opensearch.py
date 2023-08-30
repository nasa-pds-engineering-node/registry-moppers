import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)

NODE_FOLDERS = {
    "atmos": "PDS_ATM",
    "en": "PDS_ENG",
    "geo": "PDS_GEO",
    "img": "PDS_IMG",
    "naif": "PDS_NAIF",
    "ppi": "PDS_PPI",
    "rings": "PDS_RMS",
    "rs": "PDS_RS",
    "sbn": "PDS_SBN",
}


class MissingIdentifierError(Exception):
    pass


def pds4_id_field_fun(doc):
    if 'version_id' in doc:
        return doc['identifier'] + '::' + doc['version_id'][-1]
    elif 'identifier' in doc:
        return doc['identifier']
    else:
        raise MissingIdentifierError()


def get_node_from_file_ref(file_ref: str):
    file_path = os.fspath(file_ref)
    path = os.path.normpath(file_path)
    dirs = path.split(os.sep)
    return NODE_FOLDERS.get(dirs[4], "PDS_EN")


class SolrOsWrapperIter:
    def __init__(self, solr_itr, es_index, found_ids=None):
        self.index = es_index
        self.type = "_doc"
        self.id_field_fun = pds4_id_field_fun
        self.found_ids = found_ids
        self.solr_itr = iter(solr_itr)

    def __iter__(self):
        return self

    def solrDoc_to_osDoc(self, doc):
        new_doc = dict()
        new_doc['_index'] = self.index
        new_doc['_type'] = self.type

        # remove empty fields
        new_doc['_source'] = {}
        for k, v in doc.items():
            # get the node from the data string
            # for example : /data/pds4/releases/ppi/galileo-traj-jup-20230818
            if k == "file_ref_location":
                new_doc['_source']['node'] = get_node_from_file_ref(v[0])

            # manage dates
            if "date" in k:
                # only keep the latest modification date, for kibana
                if k == "modification_date":
                    v = [v[-1]]

                # validate dates
                try:
                    datetime.fromisoformat(v[0].replace("Z", ""))
                    new_doc['_source'][k] = v
                except ValueError:
                    log.warning("Date %s for field %s is invalid", v, k)
            elif "year" in k:
                if len(v[0]) > 0:
                    new_doc['_source'][k] = v
                else:
                    log.warning("Year %s for field %s is invalid", v, k)
            else:
                new_doc['_source'][k] = v

        if self.id_field_fun:
            id = self.id_field_fun(doc)
            new_doc['_id'] = id
            new_doc['_source']['found_in_registry'] = "true" if id in self.found_ids else "false"
        return new_doc

    def __next__(self):
        while True:
            try:
                doc = next(self.solr_itr)
                return self.solrDoc_to_osDoc(doc)
            except MissingIdentifierError as e:
                log.warning(str(e))

