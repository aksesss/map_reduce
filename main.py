from WordGenerator import WordGenerator
from Mapreduce import MapReduce
import os

FILE_PATH = os.path.abspath(os.getcwd()) + '\\data\\generated_data'

if __name__ == '__main__':
    WordGenerator().to_file(n_words=100, file_name=FILE_PATH)
    mr = MapReduce(f_path=FILE_PATH)
    res = mr.value_counts(n_jobs=1)
    
    print(res[:5])