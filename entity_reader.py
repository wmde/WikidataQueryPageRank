import json
import sys
import requests


class EntityReader(object):
    def __init__(self, q_ids):
        self.q_ids = q_ids

    def request_entities(self):
        url = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=' + self.q_ids
        print("requesting ", url)
        response = requests.get(url)
        return response.text

    def read_entities(self, entities):
            entities = json.loads(entities)["entities"]
            for key, item in entities.items():
                yield item

entity_reader = EntityReader(sys.argv[1])

response_body = entity_reader.request_entities()

for item_serialization in entity_reader.read_entities(response_body):
    item_numbers = {
        'id': item_serialization.get('id'),
        'labels': len(item_serialization.get('labels', {})),
        'descriptions': len(item_serialization.get('descriptions', {})),
        'aliases_count': len(item_serialization.get('aliases', {})),
        'aliases_sum': sum([len(item_serialization['aliases'][i]) for i in item_serialization.get('aliases', {})]),
        'sitelinks': len(item_serialization.get('sitelinks', {}))
    }
    claims_count = 0
    claims_sum = 0
    external_identifiers_count = 0
    external_identifiers_sum = 0
    for property_id in item_serialization.get('claims', {}):
        is_external_identifier = (item_serialization['claims'][property_id][0]['mainsnak']['datatype'] == 'external-id')
        if is_external_identifier:
            external_identifiers_count += 1
            external_identifiers_sum += len(item_serialization['claims'][property_id])
        else:
            claims_count += 1
            claims_sum += len(item_serialization['claims'][property_id])
    item_numbers['ext_ids_count'] = external_identifiers_count
    item_numbers['ext_ids_sum'] = external_identifiers_sum
    item_numbers['claims_count'] = claims_count
    item_numbers['claims_sum'] = claims_sum

    print(json.dumps(item_numbers))
