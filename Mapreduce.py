import os
import multiprocessing as mp
from collections import Counter

class MapReduce():
    def __init__(self, f_path):
        if f_path is not None: self.f_path = f_path

    def get_chunks(self, n_splits=2):
        fsize = os.path.getsize(self.f_path)
        ch_size = fsize // n_splits
        ch_start = 0
        done = False
        with open(self.f_path, 'rb') as file:
            while not done:
                file.seek(ch_size, 1)
                file.readline()
                ch_end=file.tell()

                yield ch_start, ch_end-ch_start
                ch_start = ch_end
                if (ch_start >= fsize) : 
                    done = True         

    def mapper(self, ch_start=0, ch_size=50):
        with open(self.f_path) as file:
            file.seek(ch_start)
            s = file.readlines(ch_size)
            s = [x[:-1] for x in s]
            return Counter(s)

    def reducer(self, cnt1, cnt2):
        return cnt1+cnt2

    
    def value_counts(self, n_jobs=5):
        '''
        :param n_jobs: n parallel processes
        :return: python dictionary
        '''
        pool = mp.Pool(n_jobs)
        mappers_res = []

        for ch_start, ch_size in self.get_chunks(n_splits=n_jobs):
            mappers_res.append(pool.apply_async(self.mapper, (ch_start, ch_size)))

        res = Counter()
        for mapper_res in mappers_res:
            res = self.reducer(res, mapper_res.get())

        res = sorted(res.items(), key=lambda pair: pair[1], reverse=True)
        return(res)
