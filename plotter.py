import json
import math
import random
import sys

import matplotlib.pyplot as plt
import numpy as np

data = []
with open(sys.argv[1], 'r') as f:
    for line in f.read().split('\n'):
        if not line.strip():
            continue
        if random.random() > 0.03:
            continue
        data.append(json.loads(line))

fig = plt.figure()

ax = fig.add_subplot(111, projection='3d')

x = 'labels'
y = 'sitelinks'
z = 'ext_ids'
if '--log' in sys.argv:
    xs = [math.log(i[x] + 1) for i in data]
    ys = [math.log(i[y] + 1) for i in data]
    zs = [math.log(i[z] + 1) for i in data]
else:
    xs = [i[x] for i in data]
    ys = [i[y] for i in data]
    zs = [i[z] for i in data]
ax.scatter(xs, ys, zs)

ax.set_xlabel(x)
ax.set_ylabel(y)
ax.set_zlabel(z)

plt.show()
