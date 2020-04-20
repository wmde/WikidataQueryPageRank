 #!/usr/bin/env python3

import json
import requests
import sys
import re
import os
import getopt

from SPARQLWrapper import SPARQLWrapper, JSON
from dictor import dictor

WIKIDATA_URL = "https://www.wikidata.org/w/api.php?action=wbgetentities&format=json"
SPARQL_URL = "https://query.wikidata.org/sparql"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def parse_q_url(q_url):
    # Q-id is the part behind the last '/' character in the URL
    return q_url.rsplit("/",1)[1]

class RankingSPARQLClient(object):

    def __init__(self, path, q_field):
        query_dir = os.path.dirname(path)
        query_file = os.path.basename(path)

        result_dir = os.path.join(query_dir,"../results")
        metadata_dir = os.path.join(query_dir,"../metadata")
        p = re.compile(".sparql")
        json_file = p.sub('.json', query_file)
        self.query_path = path
        self.result_path = os.path.join(result_dir, json_file)
        self.metadata_path = os.path.join(metadata_dir, json_file)

        self.q_field = q_field

    def read_query(self):
        with open(self.query_path, 'r') as file:
            sparql = file.read()
            file.close()

        return sparql

    def read_sparql_results(self):
        eprint("reading sparql query results from disk")
        with open(self.result_path, 'r') as file:
            results = json.load(file)
            file.close()
        return results

    def write_sparql_results(self, results):
        with open(self.result_path, 'w') as file:
            file.write(json.dumps(results))
            file.close()

    def get_sparql_results(self, query):
        if(os.path.isfile(self.result_path)):
            return self.read_sparql_results()

        eprint("fetching sparql query results from API")
        user_agent = "WikidataQueryPageRank (https://github.com/wmde/WikidataQueryPageRank) Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        sparql = SPARQLWrapper(SPARQL_URL, agent=user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()["results"]["bindings"]
        self.write_sparql_results(results)

        return results


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
                    q_id = parse_q_url(value)

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
            q_ids.append(parse_q_url(q_field_value))

        return q_ids

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

    def read_metadata(self):
        eprint("reading metadata from disk")
        with open(self.metadata_path, 'r') as file:
            metadata = json.load(file)
            file.close()
        return metadata

    def write_metadata(self, metadata):
        with open(self.metadata_path, 'w') as file:
            file.write(json.dumps(metadata))
            file.close()

    def collect_metadata(self, query_results):
        if(os.path.isfile(self.metadata_path)):
            return self.read_metadata()

        metadata = {}
        # collect a list of Q-ids from query results
        q_ids = self.extract_q_ids(query_results)

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

        self.write_metadata(metadata)
        return metadata

    def add_field_relevance(self, results, field):
        for result_item in results:
            result_item['meta']['relevance'] = dictor(result_item, field)

        return results

    def add_combined_field_relevance(self, results, fields):
        for result_item in results:
            result_item['meta']['relevance'] = 0
            for field in fields:
                field_value = dictor(result_item, field)
                if field_value is None:
                    eprint("Could not find ranking field '"+field+"' in result:", result_item)
                    exit(2)

                result_item['meta']['relevance'] += field_value

        return results

    def add_weighted_field_relevance(self, results, weighted_fields):
        for result_item in results:
            result_item['meta']['relevance'] = 0
            for weighted_field in weighted_fields:
                field_value = dictor(result_item, weighted_field[0])
                if field_value is None:
                    eprint("Could not find ranking field '"+weighted_field+"' in result:", result_item)
                    exit(2)

                result_item['meta']['relevance'] += field_value * weighted_field[1]

        return results

    def add_rank_by_relevance(self, results, limit=10):
        ranked_results = {}
        sorted_results = sorted(results, key=lambda x: x['meta']['relevance'],reverse=True)[:limit]
        for idx, sorted_item in enumerate(sorted_results, start=1):
            ranked_results[idx] = sorted_item

        return ranked_results

    def add_relevance(self, results, field):
        if field == 's+e':
            relevance_results = self.add_combined_field_relevance(results, ['meta.sitelinks', 'meta.ext_ids_sum'])
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results
        elif field == 's+2e':
            relevance_results = self.add_weighted_field_relevance(results, [['meta.sitelinks', 1], ['meta.ext_ids_sum', 2]])
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results
        elif field == '2s+e':
            relevance_results = self.add_weighted_field_relevance(results, [['meta.sitelinks', 2], ['meta.ext_ids_sum', 1]])
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results
        elif field == 's+l':
            relevance_results = self.add_combined_field_relevance(results, ['meta.sitelinks', 'meta.labels'])
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results
        elif field == 's+2l':
            relevance_results = self.add_weighted_field_relevance(results, [['meta.sitelinks', 1], ['meta.labels', 2]])
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results
        elif field == 'e+l':
            relevance_results = self.add_combined_field_relevance(results, ['meta.ext_ids_sum', 'meta.labels'])
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results
        elif field == 's+e+l':
            relevance_results = self.add_combined_field_relevance(results, ['meta.sitelinks', 'meta.ext_ids_sum', 'meta.labels'])
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results
        else:
            if dictor(results[0], field) is None:
                eprint("Could not find ranking field '"+field+"' in result:", results[0])
                exit(2)

            relevance_results = self.add_field_relevance(results, field)
            ranked_results = self.add_rank_by_relevance(relevance_results)
            return ranked_results

def dump_results(results):
    print(json.dumps(results, indent=2))
    eprint("found " + str(len(results)) + " results")

def get_goldranking(goldrankings, id):
    goldranking = {}
    for qid, item in goldrankings.items():
       goldranking[qid] = item[id]

    return goldranking

def get_resultranking(results):
    resultranking = {}
    for result_rank, result in results.items():
        qid = parse_q_url(result[q_field])
        resultranking[qid] = result_rank

    return resultranking



def compare_rankings(result_ranking, gold_ranking):
    score = 0
    for qid, result_rank in result_ranking.items():
        gold_rank = gold_ranking.get(qid)
        if gold_rank is None:
            score += 15
        else:
            #print(qid,"on rank", result_rank, "is off from gold standard's rank",gold_rank,"by",abs(result_rank-gold_rank))
            score += abs(result_rank-gold_rank)*(10-gold_rank)

    return score

def evaluate(result_ranking, goldrank_file):
    goldranking_ids = ['LP','SH','PG']
    with open(goldrank_file, 'r') as file:
        goldrankings = json.load(file)
        overall_score = 0

        for goldranking_id in goldranking_ids:
            gold_ranking = get_goldranking(goldrankings, goldranking_id)
            for comp_id in goldranking_ids:
                comp_score = compare_rankings(gold_ranking, get_goldranking(goldrankings, comp_id))
                #print("comparison score between", goldranking_id, "and", comp_id, "gold rankings is:", comp_score)

            score = compare_rankings(result_ranking, gold_ranking)
            #print("result's score for", goldranking_id+"'s gold ranking is:", score)
            overall_score += score

        return round(overall_score / len(goldranking_ids),1)

# default q-field
q_field = "item"
rank_by = None
goldrank_file = None

# read command line options
try:
    opts, args = getopt.getopt(sys.argv[1:],"he:q:r:",["evaluate=", "q-field=", "rank-by="])
except getopt.GetoptError:
    eprint(sys.argv[0] + '[--evaluate=<goldrankfile>] [--q-field=<qfieldname>] [--rank-by=<rankfieldname>] <queryfile>')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        eprint(sys.argv[0] + '[--evaluate=<goldrankfile>] [--q-field=<qfieldname>] [--rank-by=<rankfieldname>] <queryfile>')
        sys.exit()
    elif opt == "-e" or opt == "--evaluate":
        goldrank_file = arg
    elif opt == "-q" or opt == "--q-field":
        q_field = arg
    elif opt in ["-r", "--rank-by"]:
        rank_by = arg
queryfile = args[0]

sparql_client = RankingSPARQLClient(queryfile, q_field)

# read SPARQL query from file
query_text = sparql_client.read_query()

# fetch SPARQL results from Wikidata Query Service (or read from disk)
query_results = sparql_client.get_sparql_results(query_text)

# fetch metadata for Q-items from wbgetentities API
metadata = sparql_client.collect_metadata(query_results)

# combine query results with entity metadata
flat_results = sparql_client.combine_results(query_results, metadata)

# rank results according to command line parameter --rank-by
if rank_by is None:
    eprint("no ranking")
    dump_results(flat_results)
    sorted_results = sorted(flat_results, key=lambda x: x[q_field+'Label'].lower())
else:
    eprint("ranking by", rank_by)
    ranked_results = sparql_client.add_relevance(flat_results, rank_by)
    #dump_results(ranked_results)
    if(goldrank_file is not None):
        result_ranking = get_resultranking(ranked_results)
        mean_rankdiff_score = evaluate(result_ranking, goldrank_file)
print("mean_rankdiff_score for ranking",queryfile ,"by", rank_by, "is", mean_rankdiff_score)


