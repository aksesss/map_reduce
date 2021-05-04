import numpy as np
import string

class WordGenerator():
        
    def __init__(self):
        self.DIGS_LIST = [w for w in string.ascii_letters]

    
    def generate_word(self):
        w_len = np.random.choice(21)+30
        word = np.random.choice(self.DIGS_LIST, w_len)
        return(''.join(str(e) for e in word))
    
    def to_file(self, n_words=10, file_name='data.txt'):
        try:
            with open(file_name, 'a') as file:
                for _ in range(n_words):
                    file.write(self.generate_word() + '\n')
        except KeyboardInterrupt:
            print('Stopped')