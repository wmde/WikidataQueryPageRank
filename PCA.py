import json
import math
import random
import sys
from collections import OrderedDict

import numpy as np
from sklearn.decomposition import PCA

data = []
xyz = ['labels', 'sitelinks', 'ext_ids']
i_mapping = []
with open(sys.argv[1], 'r') as f:
    for line in f.read().split('\n'):
        if not line.strip():
            continue
        line = json.loads(line)
        data.append([math.log(line[d] + 1) for d in xyz])
        #data.append([line[d] for d in xyz])
        i_mapping.append(line['id'])
#means = []
#stds = []
# for i in range(len(data[0])):
#    means.append(
#        np.mean([j[i] for j in data])
#    )
#    stds.append(
#        np.std([j[i] for j in data])
#    )
#new_data = []
# for case in data:
#    new_data.append([(case[i] - means[i])/stds[i] for i in range(len(case))])
#print(means, stds)
pca = PCA(n_components=1)
normalized_data = pca.fit_transform(data)
print(pca.components_, pca.mean_)
values = {}
for i in range(len(data)):
    values[i] = math.sqrt(sum([j**2 for j in normalized_data[i]]))

values = OrderedDict(sorted(values.items(), key=lambda t: t[1], reverse=True))
c = 0
for value in values:
    if c > 100:
        break
    c += 1
    print([round(math.exp(i)) for i in data[value]], i_mapping[value])
    #print(data[value], i_mapping[value])
