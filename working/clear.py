import os 
import shutil

location = "/home/metalllines/metalldata/working/"
dir ="jframe-1_4"
path = os.path.join(location, dir)
 
# removing directory
shutil.rmtree(path)