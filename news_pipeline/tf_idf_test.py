from sklearn.feature_extraction.text import TfidfVectorizer
doc1 = "hey"
doc2 = "you"
doc3 = "are"
doc4 = "awsome"
document = [doc1, doc2, doc3, doc4]
tfidf = TfidfVectorizer().fit_transform(document)
pairwise_sim = tfidf * tfidf.T 

print(pairwise_sim.A)