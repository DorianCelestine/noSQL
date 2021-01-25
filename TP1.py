#!/usr/bin/env python
# coding: utf-8

import json

#Connection a la BD
from pymongo import MongoClient
client = MongoClient('localhost',27017)
db = client.metflix
collection = db.movies
print('connection success')


collection.find_one({"title" : "Rocky"})

items = collection.find({"title" : "Rocky"})

for item in items:
    print(item)

client.drop_database('metflix')

metflix = client['metflix']

rocky = {"title" : "Rocky", "year" : 1976} 
collection.insert_one(rocky)

collection.find_one({"title" : "Rocky"})

collection.size

print(collection.count())

collection.estimated_document_count()

#redondance de rocky
collection.insert_one(rocky)

rocky = {"title" : "Rocky", "year" : 1976, "_id" : "tt0075148"}
collection.insert_one(rocky)

collection.estimated_document_count()

dict = {"nom" : "Celestine", "prenom" : "Dorian"}

users = db["users"]

users.insert_one(dict)
#impossible, redondance de l'objet

users.drop()
users = db["users"]

#peut être fait avec un try exept
users.insert_many([
    {"title" : "Rocky" , "year" : 1976 , "_id" : "tt0075148"},
    {"title" : "Jaws" , "year" : 1975 , "imdb" : "tt0073195"},
    {"title" : "Mad Max 2 : The Road Warrior" , "year" : 1981 , "imdb" : "tt0082694"},
    {"title" : "Raiders of the Lost Ark" , "year" : 1981 , "imdb" : "tt0082971"}
])


#des tests
collection.find_one()

collection.find()

cur = collection.find({"year" : 1976})
#fait via le pointeur initialisé juste au dessus
for movie in cur:
    print(movie)

collection.find_one()

for user in users.find({"year" : 1981}) :
    print(user)

users.find_one({"title" : "Jaws"})


#BD 2 -------------------------

#Connection a la BD
from pymongo import MongoClient
#le import json est tout en haut
client = MongoClient('localhost',27017)
db2 = client.movies_artists
with open('movies.json') as f:
    filedata = json.load(f)
with open('artists.json') as f2:
    filedata2 = json.load(f2)
print('connection success')

#drop et insert des json
db2.drop()
db2 = db2.insert_many(filedata)
db2 = db2.insert_many(filedata2)

for x in db2.find({},{"_id" : 0, "title" : 1}).limit(12).sort("title",1):
    print(x)

for x in db2.find({},{"_id" : 0, "title" : 1}).limit(12).sort("title",-1):
    print(x)

v1979 = db2.find({"year" : 1979},{"_id" : 0, "title" : 1, "year" : 1}).count()
print(v1979)

