import re
# Regular expression for removing all non-letter characters in the file.
regex = re.compile('[^a-zA-Z ]')


'''
Removes any non-letter character from the given word.

INPUT:
        word: A word

OUTPUT:
        the input word without the non-letter characters.

'''
def remove_non_letters(word):
    return regex.sub('', word)


'''
INPUT: 
        stopwords_file: name of the file containing the stopwords.
OUTPUT:
        a Python list with the stopwords read from the file.
'''
def load_stopwords(stopwords_file):
    stopwords = []
    with open(stopwords_file) as file:
        for sw in file:
            stopwords.append(sw.strip())
    return stopwords


'''
INPUT: 
        text: RDD where each element is a line of the input text file.
        stopwords: Python list containing the stopwords.
OUTPUT: 
        RDD where each element is a word from the input text file.
'''
def preprocess(text, stopwords) :
  words = text.flatMap(lambda line: line.split(" ")).map(lambda word: remove_non_letters(word)).filter(lambda word: len(word) > 0).map(lambda word: word.lower()).filter(lambda word: word not in stopwords)
  return words

'''
Returns how many times a word appears in a RDD 
INPUT:
        words: RDD, where each element is word from the input text file (preprocessing already done!).
OUTPUT:
        RDD, where each element is (w, occ), w is a word and occ the number of occurrences of w.
        The RDD is sorted by value in decreasing order.
'''

def word_count(words):    
    occs = words.map(lambda word: (word, 1))\
                .reduceByKey(lambda x, y: x+y)\
                .sortBy(lambda f: f[1], ascending=False)
    return occs

# Storing in stopwords the list of the stopwords that is provided
stopwords = load_stopwords("./data/stopwords.txt")
