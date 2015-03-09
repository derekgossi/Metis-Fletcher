import pickle

# Open heavy metal reviews dataframe 
with open("heavy_metal_parsed.pkl", 'r') as datafile:
    hm_reviews = pickle.load(datafile)

# Initialize MongoDB client
from pymongo import MongoClient

client = MongoClient()

# Insert reviews into DB
hmm = client.dsbc.hmm
for review in hm_reviews:
    hmm.save(review)


### CHALLENGE #1

import matplotlib.pyplot as plt
import seaborn as sb
%matplotlib inline

# Get cursor of years
hmm_yr_c = hmm.find( {} , { 'year' } )

hmm_yrs = [ ele['year'] for ele in hmm_yr_c ]

# Plot histogram of years
plt.hist(hmm_yrs)
plt.show()


### CHALLENGE #2

from collections import Counter

# Get cursor of cast members
hmm_cast_c = hmm.find( {} , { 'cast' } )

# Count cast members
cast_cnt = Counter()
for ele in hmm_cast_c:
    for cast_member in ele['cast']:
        cast_cnt[cast_member] += 1
   
# Sort and print 
srt_cast_cnt = sorted( cast_cnt.items(), key=operator.itemgetter(1), reverse=True )
print srt_cast_cnt[1:10]


### CHALLENGE #3

# Get cursor of title
hmm_title_c = hmm.find( {} , { 'title' } )

title_wrd_cnt = Counter()

# Count title words
for ele in hmm_title_c:
    for word in ele['title'].split():
        title_wrd_cnt[word] += 1

# Sort and print 
srt_title_wrd_cnt = sorted( title_wrd_cnt.items(), key=operator.itemgetter(1), reverse=True )
print srt_title_wrd_cnt[1:10]

### CHALLENGE #4

from bson.code import Code
import operator

map = Code(
	"function () { "   
    		"var titleSplitted = this.title.split(' ');"
    		"titleSplitted.forEach(function(z){"
  		 	"emit(z, 1);"    
   		"});"
	"};"
	)

reduce = Code(
	"function (key, values) {"
       	"var total = 0;"
              "for (var i = 0; i < values.length; i++) {"
               	"total += values[i];"
            	"}"
       "return total;"
       "};"
       )

result = hmm.map_reduce(map, reduce, "myresults")

word_dict={}
for doc in result.find():
	word_dict[doc["_id"]]=doc["value"]
print sorted(word_dict.items(), key=operator.itemgetter(1))[-5:]