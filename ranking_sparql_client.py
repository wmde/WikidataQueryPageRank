import json
import requests
import sys
import getopt
from SPARQLWrapper import SPARQLWrapper, JSON
from dictor import dictor

WIKIDATA_URL = "https://www.wikidata.org/w/api.php?action=wbgetentities&format=json"
SPARQL_URL = "https://query.wikidata.org/sparql"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class RankingSPARQLClient(object):

    def __init__(self, path, q_field):
        self.path = path
        self.q_field = q_field

    def read_query(self):
        with open(self.path, 'r') as file:
            sparql = file.read()
        return sparql

    def query_sparql(self, query):
        user_agent = "WikidataQueryPageRank (https://github.com/wmde/WikidataQueryPageRank) Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        sparql = SPARQLWrapper(SPARQL_URL, agent=user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()["results"]["bindings"]

    def combine_results(self, query_results, metadata):
        combined_results = []
        for result in query_results:
            q_id = ""
            combined_result = {}

            # collect values for each field in result
            for field in result.keys():
                value = result.get(field).get("value")
                combined_result[field] = value
                # note the Q-id from known q-field
                if field == self.q_field:
                    q_id = self.parse_q_url(value)

            if q_id == "":
                # no field with Q-id found in this result
                continue

            # enhance flat result with metadata
            combined_result["meta"] = {}
            for metafield in ['labels', 'descriptions', 'aliases_sum', 'sitelinks', 'ext_ids_sum', 'claims_sum']:
                combined_result["meta"][metafield] = metadata[q_id][metafield]

            # append combined result to return list
            combined_results.append(combined_result)

        return combined_results

    def extract_q_ids(self, results):
        q_ids = []
        for result in results:
            if self.q_field not in result:
                if self.q_field == 'item':
                    eprint("Cannot find Q-item field in results. Please specify via --q-field=<fieldname>")
                    exit(2)
                else:
                    eprint("Cannot find item field '" + self.q_field + "' in results.")

                # no field with Q-id found in this result
                continue

            q_field_value = result[self.q_field]['value']
            q_ids.append(sparql_client.parse_q_url(q_field_value))

        return q_ids

    def parse_q_url(self, q_url):
        # Q-id is the part behind the last '/' character in the URL
        return q_url.rsplit("/",1)[1]

    def request_entities(self, q_ids):
        # the API endpoint doesn't accept more than 50 Q-ids, so
        # if necessary, we split here and do multiple requests in a row
        chunks = [q_ids[n:n+50] for n in range(0, len(q_ids), 50)]
        for chunk in chunks:
            url = WIKIDATA_URL + "&ids=" + "|".join(chunk)
            eprint("requesting", url)
            response = requests.get(url)
            entities = json.loads(response.text)["entities"]
            for key, item in entities.items():
                yield item

    def collect_metadata(self, q_ids):
        metadata = {}
        for item in sparql_client.request_entities(q_ids):
            item_metadata = {
                'labels': len(item.get('labels', {})),
                'descriptions': len(item.get('descriptions', {})),
                'aliases_count': len(item.get('aliases', {})),
                'aliases_sum': sum([len(item['aliases'][i]) for i in item.get('aliases', {})]),
                'sitelinks': len(item.get('sitelinks', {}))
            }
            claims_count = 0
            claims_sum = 0
            external_identifiers_count = 0
            external_identifiers_sum = 0
            for property_id in item.get('claims', {}):
                is_external_identifier = (item['claims'][property_id][0]['mainsnak']['datatype'] == 'external-id')
                if is_external_identifier:
                    external_identifiers_count += 1
                    external_identifiers_sum += len(item['claims'][property_id])
                else:
                    claims_count += 1
                    claims_sum += len(item['claims'][property_id])
            item_metadata['ext_ids_count'] = external_identifiers_count
            item_metadata['ext_ids_sum'] = external_identifiers_sum
            item_metadata['claims_count'] = claims_count
            item_metadata['claims_sum'] = claims_sum

            metadata[item.get('id')] = item_metadata

        return metadata

    def rank_results(self, results, field):
        if dictor(results[0], field) is None:
            eprint("Could not find ranking field '"+field+"' in result:", results[0])
            exit(2)

        return sorted(results, key=lambda x: dictor(x, field),reverse=True)

# default q-field
q_field = "item"
rank_by = "item"

# read command line options
try:
    opts, args = getopt.getopt(sys.argv[1:],"hq:r:",["q-field=", "rank-by="])
except getopt.GetoptError:
    print(sys.argv[0] + '[--q-field=<qfieldname>] <queryfile>')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print(sys.argv[0] + '[--q-field=<fieldname>] <queryfile>')
        sys.exit()
    elif opt in ("-q", "--q-field"):
        q_field = arg
    elif opt in ("-r", "--rank-by"):
        rank_by = arg
queryfile = args[0]

sparql_client = RankingSPARQLClient(queryfile, q_field)

# read SPARQL query from file
query_text = sparql_client.read_query()

# send SPARQL to Wikidata Query Service
query_results = sparql_client.query_sparql(query_text)

# collect a list of Q-ids from query results
q_ids = sparql_client.extract_q_ids(query_results)

# fetch metadata for Q-items from wbgetentities API
metadata = sparql_client.collect_metadata(q_ids)

# combine query results with entity metadata
flat_results = sparql_client.combine_results(query_results, metadata)

# rank results according to command line parameter --rank-by
ranked_results = sparql_client.rank_results(flat_results, rank_by)

print(json.dumps(ranked_results, indent=2))
eprint("found " + str(len(flat_results)) + " results")


