#!/usr/bin/env python

# This file is part of Living Labs Challenge, see http://living-labs.net.
#
# Living Labs Challenge is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Living Labs Challenge is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Living Labs Challenge. If not, see <http://www.gnu.org/licenses/>.

import argparse
import requests
import json
import time
import random
import os
from nordlys.retrieval import indexer

QUERYENDPOINT    = "participant/query"
DOCENDPOINT      = "participant/doc"
DOCLISTENDPOINT  = "participant/doclist"
RUNENDPOINT      = "participant/run"
FEEDBACKENDPOINT = "participant/feedback"
HISTORICALENDPOINT = "participant/historical"
HEADERS = {'content-type': 'application/json'}

class Participant():
	def __init__(self):
		path = os.path.dirname(os.path.realpath(__file__))
		description = "Living Labs Challenge's Participant Client"
		parser = argparse.ArgumentParser(description=description)
		parser.add_argument('--host', dest='host',
							default='http://living-labs.net',
							help='Host to listen on.')
		parser.add_argument('--port', dest='port', default=5000, type=int,
							help='Port to connect to.')
		parser.add_argument('-k', '--key', type=str, required=True,
							help='Provide a user key.')
		parser.add_argument('-s', '--simulate_runs', action="store_true",
							default=False,
							help='Simulate runs.')
		parser.add_argument('--store_run', action="store_true",
							default=False,
							help='Store TREC run (needs --run_file).')
		parser.add_argument('--run_file',
							default=os.path.normpath(os.path.join(path,
													 "../../data/run.txt")),
							help='Path to TREC style run file '
							'(default: %(default)s).')
		parser.add_argument('--get_feedback', action="store_true",
							default=False,
							help="Get feedback, if any")
		parser.add_argument('--reset_feedback', action="store_true",
							default=False,
							help="Get feedback, if any")
		parser.add_argument('--wait_min', type=int, default=1,
							help='Minimum simulation waiting time in seconds.')
		parser.add_argument('--wait_max', type=int, default=10,
							help='Max simulation waiting time in seconds.')

		args = parser.parse_args()
		self.key = args.key
		self.host = "%s:%s/api" % (args.host, args.port)
		if not self.host.startswith("http://"):
			self.host = "http://" + self.host

		self.runid = 0
		
		if args.store_run:
			self.store_run(args.key, args.run_file)

		if args.get_feedback:
			self.get_feedbacks(args.key)

		if args.reset_feedback:
			self.reset_feedback(args.key)

		if args.simulate_runs:
			self.simulate_runs(args.key, args.wait_min, args.wait_max)

	def get_queries(self):
		url = "/".join([self.host, QUERYENDPOINT, self.key])
		r = requests.get(url, headers=HEADERS)
		time.sleep(random.random())
		if r.status_code != requests.codes.ok:
			print r.text
			r.raise_for_status()
		
		return r.json()
	
	def get_doclist(self,qid):
		url = "/".join([self.host, DOCLISTENDPOINT, self.key, qid])
		r = requests.get(url, headers=HEADERS)
		time.sleep(random.random())
		if r.status_code != requests.codes.ok:
			print r.text
			r.raise_for_status()
		return r.json()


	def get_document(self,docid):
		url = "/".join([self.host, DOCENDPOINT, self.key, docid])
		r = requests.get(url, headers=HEADERS)
		time.sleep(random.random())
		if r.status_code != requests.codes.ok:
			print r.text
			r.raise_for_status()
		return r.json()
		
		

	# if qid == "all" returns feedback for all queries
	def get_feedback(self, qid, runid=None):
		urlList = [self.host, FEEDBACKENDPOINT, self.key, qid]
		if runid:
			urlList.append(str(runid))
		url = "/".join(urlList)
		r = requests.get(url, headers=HEADERS)
		time.sleep(random.random())
		if r.status_code != requests.codes.ok:
			print r.text
			r.raise_for_status()
		return r.json()

	def reset_feedback(self):
		queries = self.get_queries()
		for query in queries["queries"]:
			qid = query["qid"]
			url = "/".join([self.host, FEEDBACKENDPOINT, self.key, qid])
			r = requests.delete(url, headers=HEADERS)
			time.sleep(random.random())
			if r.status_code != requests.codes.ok:
				print r.text
				r.raise_for_status()

	def historical_feedback(self,qid):
		urlList = [self.host, HISTORICALENDPOINT, self.key, qid]
		url = "/".join(urlList)
		r = requests.get(url, headers=HEADERS)
		time.sleep(random.random())
		if r.status_code != requests.codes.ok:
			print r.text
			r.raise_for_status()
		return r.json()


	def store_runs(self, runs):
		for qid in runs:
			run = runs[qid]
			run["runid"] = str(self.runid)
			url = "/".join([self.host, RUNENDPOINT, self.key, qid])
			print 'submitting ...%s'% qid 
			r = requests.put(url, data=json.dumps(run), headers=HEADERS)
			time.sleep(random.random())
			if r.status_code != requests.codes.ok:
				print r.text
				r.raise_for_status()

		print "Your runs er submitted ...."

	def update_runs(self,runs, feedbacks):
		for qid in runs:
			if qid in feedbacks and feedbacks[qid]:
				clicks = dict([(doc['docid'], 0) for doc in runs[qid]['doclist']])
				for feedback in feedbacks[qid]:
					for doc in feedback["doclist"]:
						if doc["clicked"] and doc["docid"] in clicks:
							clicks[doc["docid"]] += 1
				runs[qid]['doclist'] = [{'docid': docid}
										for docid, _ in
										sorted(clicks.items(),
											   key=lambda x: x[1],
											   reverse=True)]
				print clicks
		self.runid += 1
		self.store_runs(runs)
		return runs

	def update_runid(self, old_runid):
		try:
			while int(old_runid) >= self.runid:
				self.runid += 1
		except ValueError:
			pass

	def simulate_runs(self, wait_min, wait_max):
		queries = self.get_queries()
		runs = {}
		for query in queries["queries"]:
			qid = query["qid"]
			runs[qid] = self.get_doclist(qid)
		feedbacks = {}
		feedback_update = self.get_feedback(self.key, "all")
		for elem in feedback_update['feedback']:
			self.update_runid(elem["runid"])
		while True:
			for elem in feedback_update['feedback']:
				qid = elem["qid"]
				if qid in feedbacks:
					feedbacks[qid].append(elem)
				else:
					feedbacks[qid] = [elem]
			#runs = self.update_runs(runs, feedbacks)
			time.sleep(wait_min + (random.random() * (wait_max - wait_min)))
			for qid, doclists in feedbacks.items():
				for doclist in doclists:
					print qid, " ".join([doc["docid"] for doc in doclist
									 if doc["clicked"]])

			feedback_update = self.get_feedback("all", self.runid)
	"""
	:filter run against given doclist
	"""
	def store_run(self,run_file):
		runs = {}
		nominees = {}
		current_qid = None
		for line in open(run_file, "r"):
			qid, _, docid, _, _, _ = line.split()
			if current_qid is None or current_qid != qid:
				runs[qid] = {"doclist": []}
			#filter results that only occur in doclist
			if not nominees or current_qid != qid:
				nominees = self.get_doclist(qid)
			
			for doc_nominee in nominees['doclist']:
				if len(nominees['doclist']) == 1:
					runs[qid]['doclist'].append({"docid":doc_nominee['docid']})
				elif doc_nominee['docid'] == docid:
					runs[qid]['doclist'].append({"docid":docid})
					break
			current_qid = qid
		
		self.store_runs(runs)

	def get_feedbacks(self, key):
		feedbacks = {}
		for elem in self.get_feedback(key, "all")['feedback']:
			qid = elem["qid"]
			if qid in feedbacks:
				feedbacks[qid].append(elem["doclist"])
			else:
				feedbacks[qid] = [elem["doclist"]]
		for qid, doclists in feedbacks.items():
			for doclist in doclists:
				print qid, " ".join([doc["docid"]
									 for doc in doclist
									 if doc["clicked"]])
	"""
	:param qid-query id and  team - participant or cite
	:output - {docid:#clicks} for query qid
	"""
	def multiple_feedbacks(self,qid,team):
		clicks = {}

		for elem in self.get_feedback(qid)['feedback']:
			for doc in elem['doclist']:
				
				if not doc['clicked'] and (doc['team'] == team) :
					docid = doc['docid'] 
					if docid in clicks:
						clicks[docid] += 1
					else:
						clicks[docid] = 1

		return clicks


	# filter unique documents from queries doclists            
	def get_unique_documents(self,doclists):
		unique_documents = []
		unique_doc_ids = []
		for qid in doclists:
			for qlist in doclists[qid]["doclist"]:
				if qlist["docid"] not in unique_doc_ids:
					unique_doc_ids.append(qlist["docid"])
					#unique_documents.append(qlist)

		return unique_doc_ids 

	"""
	:param - output file
	:creates qrels from historical feedback
	"""
	def prepare_qrels(self,filename):
		print 'preparing qrels file'
		with open('data/'+filename, 'w') as out:
			historic = self.historical_feedback("all")
			for histo_feed in historic['feedback']:
				docids = []
				for doc in histo_feed['doclist']:
					if doc['docid'] not in docids:
						out.write(str(histo_feed['qid'] )+ " Q0 " + str(doc['docid']) + " " + str(float(doc['clicked']) * 100)+ "\n")
    					docids.append(doc['docid'])


	"""
    :param - output jsonfile, queries 
    :write json queries to file 
    :format {'query': 'Lego', 'query_id' :'R-q23'}
	"""
	def prepare_json_queries(self,allqueries,jsonfile):
		qlist = []
		for query in allqueries['queries']:
			q = {}
			if query['qstr']:
				q['query'] = query['qstr'] 
				q['query_id'] = query['qid']
				qlist.append(q)
		with open('data/'+ jsonfile, 'w') as f:
			json.dump(qlist,f)
    

	"""
		:returns  all unique documents for Indexing
		: Make fields visible for indexing 
	"""
	def prepare_dox(self,unique_doc_ids):
		
		alldox = []
		for docid in unique_doc_ids :
			print "getting doc %s" % docid
			adoc = self.get_document(docid)
			for f in adoc['content']:
				adoc[f] = adoc['content'][f]
				adoc['characters'] = " ".join(adoc['characters'])
				weighted_query = proportionate_query(adoc['queries']) if adoc['queries'] else ""
				adoc['queries'] = weighted_query 
				del adoc['content']
				alldox.append(adoc)
		return alldox

def proportionate_query(doc_queries):
	repeated_terms = ""
	for query in doc_queries:
		repeated_terms = query + " "
		prob = float(doc_queries[query])
		repeated_terms = repeated_terms * int(round(prob)) * 100 

	return repeated_terms

def main():
	
	participant = Participant()
	#participant.store_run(run_file)
	#print 'run uploaded'
	
	participant = Participant()
	print "getting  queries ..."
	all_queries = participant.get_queries()
	print "Number of queries : %d" % len(all_queries['queries'])
	
	for query in all_queries["queries"]:
		qid = query["qid"]
		print 'getting doclist for %s'%qid
		doclists[qid] = participant.get_doclist(qid)
	
	
	unique_doc_ids = participant.get_unique_documents(doclists)
	alldox = participant.prepare_dox(unique_doc_ids)	
	print "Indexing documents..."
	indexer.lucene_indexer(alldox)
	

if __name__ == '__main__':
	main()


