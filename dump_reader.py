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


with open('external_idefs.json', 'r') as f:
    ext_ids = json.loads(f.read())

dump_reader = DumpReader(sys.argv[1])
for item_serialization in dump_reader.read_items():
    item_numbers = {
        'id': item_serialization.get('id'),
        'labels': len(item_serialization.get('labels', {})),
        'descriptions': len(item_serialization.get('descriptions', {})),
        'aliases': len(item_serialization.get('aliases', {})),
        'aliases_wow': sum([len(item_serialization['aliases'][i]) for i in item_serialization.get('aliases', {})]),
        'sitelinks': len(item_serialization.get('sitelinks', {}))
    }
    ext_id_claims = []
    non_ext_id_claims = []
    for pid in item_serialization.get('claims', {}):
        if pid in ext_ids:
            ext_id_claims.append(len(item_serialization['claims'][pid]))
        else:
            non_ext_id_claims.append(len(item_serialization['claims'][pid]))
    item_numbers['ext_ids'] = len(ext_id_claims)
    item_numbers['ext_ids_wow'] = sum(ext_id_claims)
    item_numbers['non_ext_ids'] = len(non_ext_id_claims)
    item_numbers['non_ext_ids_wow'] = sum(non_ext_id_claims)

    print(json.dumps(item_numbers))
