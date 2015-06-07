"""
Toy indexer example.

@author: Krisztian Balog
"""

import sys
from lucene_tools import Lucene

indexed_fields = ['product_name','title' ,'brand','short_description','description','characters','category','main_category','queries']
def lucene_indexer(docs):
    index_dir="/livinglabs_index"    
    lucene = Lucene(index_dir)
    lucene.open_writer()
    for doc in docs:
        contents = []
        doc[Lucene.FIELDNAME_CONTENTS] = ""
        
        print "Indexing document ID " + str(doc['docid'])
        # create content field
        for f in doc:
            if (f in indexed_fields):
                doc[Lucene.FIELDNAME_CONTENTS] = doc[Lucene.FIELDNAME_CONTENTS] +" "+ str(doc[f]) 
       
        for f in doc:
            #if f in indexed_fields:
            field_name = Lucene.FIELDNAME_ID if f == "docid" else f
            field_type = Lucene.FIELDTYPE_ID if f == "docid" else Lucene.FIELDTYPE_TEXT_TVP
            contents.append({'field_name': field_name,
                             'field_value': str(doc[f]),
                             'field_type': field_type})
        lucene.add_document(contents)

    lucene.close_writer()

