import gzip
import json
import sys


class DumpReader(object):
    def __init__(self, path):
        self.path = path

    def check_data(self, line):
        try:
            item = json.loads(line.replace('\n', '')[:-1])
        except:
            # It's not using json-lines, it's a big json thus this mess.
            return
        if item['type'] != 'item':
            return
        return item

    def read_items(self):
        with gzip.open(self.path, 'rt') as f:
            for line in f:
                item = self.check_data(line)
                if item is None:
                    continue

                yield item

dump_reader = DumpReader(sys.argv[1])
for item_serialization in dump_reader.read_items():
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
