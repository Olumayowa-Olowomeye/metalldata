
import os
import sys
import shutil   
import json
import matplotlib.pyplot as plt
from clippy import clippy_import, config

clippy_import("/home/metalllines/metalldata/build/src")
clippy_import("/home/metalllines/metalldata/build/src/MetallJsonLines")


## open jsonlines object and import data
mjl = MetallJsonLines("/home/metalllines/metalldata/working/jframe-1_4")

mjl.read_json("/home/metalllines/metalldata/working/data.ndjson")


gt = mjl[mjl.keys.subreddit == "greentext", mjl.keys.score <= 351]
print(gt.unique(columns = ['score','author']))
x = gt.describe(columns= ['author'], num = 10)
print(x)

# X_dict = dict(x)
# plt.bar(X_dict.keys(), X_dict.values(), align='center')
# plt.savefig('/home/metalllines/metalldata/working/temp.png')