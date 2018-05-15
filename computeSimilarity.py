import collections
import sys
import re
import csv
import itunes
import time
import pandas as pd
from nltk.corpus import stopwords
from sklearn.feature_extraction import FeatureHasher
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

#(value was chosen according to the tradeoff between running time and quality of results)
NUM_OF_FEATURES = 900	#num of features to the hashing trick. 

cachedStopWords = stopwords.words("english")	# common words in English (e.g to, the, etc.)

dictionary = set()	#set of words used as the dictionary	

bundleIdToDescriptions = {}	# key = bundleId, value = dict(bags of words:count) according to the app discreption

corpus = []	# build the corpus only consider words in the dictionary
keyToNum = {}	#bundleID to num in final matrix
numToKey = {}	#num to bundleID in final matrix (for testing)


def main(argv):
	"""
	Main function of the script. (Measure total time of computation and present result is optional) 
    ----------
    argv : the program arguments
    	argv[1]: file name - the file to parse (required)
    	argv[2]: -verbose - present more info (optional)
    	argv[3]: required bundleId (optional)
    	argv[4]: number of results to present (optional)
    """
	if len(argv) < 2:
		print 'No file name to parse was avilable'
		return 
	start = time.time()
	readDescriptions(argv[1])
	weightWords()
	featureHasher = buildFeatureHasher()
	results = computeSimilarity(featureHasher)
	done = time.time()
	elapsed = done - start
	if (len(argv) >= 5) and (argv[2] == '-verbose'):
		print 'total time of computation: %.5f' % elapsed	#print total time of computation
		printTopK(argv[3],int(argv[4]), results)	#show top k result for a given bundleID


def readDescriptions(file):
	"""
	Get the description and bundle name of all itunes IDs in the given file.
	For each description, build the corresponding bag-of-words along with it tf score.
	In addition, build the later-used dictionary.
    ----------
    file : the file to parse
    """
	with open(file, 'r') as Ids:
		for line in Ids:
			ID = int(line)
			item = itunes.lookup(ID)
			description = item.description.lower()	#avoid counting the same word differntly
			# remove stop words
			description = ' '.join([word for word in description.split() if word not in cachedStopWords])
			description = re.sub("[^a-zA-Z]", " ",  description)
			description = ' '.join(description.split())	#remove white spaces
			corpus.append(description)	#adding to corpus
			wordList = set(description.split())
			dictionary.update(wordList)	#update dictionary with new words

			bagOfWords = collections.Counter(re.findall(r'\w+', description))	#create the corresponding bag of words
			
			#tf scores
			factor=1.0/sum(bagOfWords.itervalues())
			for k in bagOfWords.keys():
				bagOfWords[k] = bagOfWords[k]*factor	#standart tf score
			bundleIdToDescriptions[item.json['bundleId']] = bagOfWords

def weightWords():
	"""
    The function computes the idf score for each word, then uses the idf scores to weight the words
    in each description bag-of-words.
    """

	#compute idf score to each word in dictionary 
	vectorizer = TfidfVectorizer(vocabulary = dictionary,stop_words = cachedStopWords,max_features =len(dictionary))
	X = vectorizer.fit_transform(corpus)
	idf = vectorizer._tfidf.idf_
	idfDict = dict(zip(vectorizer.get_feature_names(), idf))

	#weight the bag-of-words with idf scores, obtaining tf-idf score for each word in each description
	for (k,v) in bundleIdToDescriptions.items():
		for (word,count) in v.items():
			factor = idfDict[word]
			v[word] = v[word]*factor
		bundleIdToDescriptions[k] = v



def buildFeatureHasher():
	"""
	Build the feature hasher from the weighted matrix (bag-of-words weighted by the tf-idf scores of each description)
    ----------
    Return : the feature hasher
    """
	hasher = FeatureHasher(n_features=NUM_OF_FEATURES)	#use the hashing trick
	#the tf-idf scores for each description
	D = []	#holder of the normalized BOWs (not a matrix, list of lists)
	i = 0
	for (k,v) in bundleIdToDescriptions.items():
		keyToNum[k] = i
		numToKey[i] = k
		i = i +1
		D.append(v)
	featureHasher = hasher.transform(D) #transform D into (#BOW X NUM_OF_FEATURES) matrix, using the hashing trick.
	featureHasher.toarray()
	return featureHasher

def computeSimilarity(featureHasher):
	"""
	Compute pair-wise similarity scores using cosine similarity.
	Write the results to results.csv
    ----------
    featureHasher : the array of vectors (used as the similarity function input) 
    """
	results = cosine_similarity(featureHasher)	#compute pair-wise cosine
	#write results to results.csv
	bundles = keyToNum.items()
	sortedBundles = sorted(bundles, key=lambda x: x[1])
	sortedBundles = [i[0] for i in sortedBundles]
	df = pd.DataFrame(results, index=sortedBundles, columns=sortedBundles)
	df.to_csv('results.csv', index=True, header=True, sep=',')
	return results

def printTopK(bundleId,k,results):
	"""
	For testing: present the top k similar bundleIds for a given bundleId
    ----------
    bundleId : the query bundleId
    k: number of results to present
    results: the pair-wise similarity scores computed 
    """
	num = keyToNum[bundleId]	#get bundleId num
	relevant = results[num]	#extract only the current row
	sortByIndex = [i[0] for i in sorted(enumerate(relevant), key=lambda x:x[1], reverse = True)]
	print 'top %d similar apps for %s'% (k, bundleId)
	for i in range(k):
		j = i+1
		print '%d) %s'% (j, numToKey[sortByIndex[i]])


if __name__ == "__main__":
    main(sys.argv)


