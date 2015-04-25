"""
Various retrieval models for scoring a individual document for a given query.

@author: Krisztian Balog
"""

from __future__ import division
import math
from lucene_tools import Lucene
from org.apache.lucene.analysis.tokenattributes import CharTermAttribute
from org.apache.lucene.search import CollectionStatistics


class Scorer(object):
    """Base scorer class."""

    SCORER_DEBUG = 0

    def __init__(self, lucene, query, params):
        self.lucene = lucene
        self.query = query
        self.params = params
        self.lucene.open_searcher()
        """
        @todo consider the field for analysis
        """
        self.query_terms = self.analyze_query()

    def analyze_query(self):
        """Analyses the query.

        NOTE: The analyser might return terms that are not in the collection.
              These terms are filtered out later in the scorer.

        :return list of query terms
        """
        qterms = []  # holds a list of analyzed query terms
        ts = self.lucene.get_analyzer().tokenStream(Lucene.FIELDNAME_CONTENTS, self.query)
        term = ts.addAttribute(CharTermAttribute.class_)
        ts.reset()
        while ts.incrementToken():
            qterms.append(term.toString())
        ts.end()
        ts.close()
        return qterms

    @staticmethod
    def get_scorer(model, lucene, query, params):
        """Returns Scorer object (Scorer factory).

        :param model: accepted values: lucene, lm or mlm
        :param lucene: Lucene object
        :param query: raw query (to be analyzed)
        :param params: dict with models parameters
        """
        if model == "lm":
            return ScorerLM(lucene, query, params)
        elif model == "mlm":
            return ScorerMLM(lucene, query, params)
        else:
            raise Exception("Unknown model '" + model + "'")


class ScorerLM(Scorer):
    """LM scorer."""

    def __init__(self, lucene, query, params):
        super(ScorerLM, self).__init__(lucene, query, params)
        self.smoothing_param = self.params.get('smoothing_param', 0.1)

    def get_term_probs(self, lucene_doc_id, field):
        """ Returns probability of each term for the given field using JM smoothing
        i.e. for each term: p(t|theta_d_f) = [(1-lambda) n(t, d_f)/|d_f|] + [lambda n(t, C_f)/|C_f|]

        :param lucene_doc_id: internal Lucene document ID
        :param field: entity field name, e.g. <dbo:abstract>
        :return: dictionary of terms with their probabilities
        """
        if self.params.get('smoothing_method', "jm") != "jm":
            raise Exception("Err: Only JM smoothing is supported!")

        # Gets term freqs for field of document
        # If the document is not in the index, all freqs are zero
        doc_term_freqs = {}
        if lucene_doc_id is not None:
            doc_term_freqs = self.lucene.get_doc_termfreqs(lucene_doc_id, field)

        # Gets term probabilities
        len_d_f = sum(doc_term_freqs.values())
        p_t_theta_d_f = {}  # holds smoothed term probabilities for the document field
        len_C_f = self.lucene.get_coll_length(field)
        for t in set(self.query_terms):
            # p(t|theta_e_f) = [(1-lambda) n(t, e_f)/|e_f|] + [lambda n(t, C_f)/|C_f|]
            doc_term_freq = doc_term_freqs.get(t, 0)
            p_t_d_f = doc_term_freq / len_d_f if len_d_f != 0 else 0
            coll_term_freq = self.lucene.get_coll_termfreq(t, field)
            p_t_C_f = coll_term_freq / len_C_f if len_C_f > 0 else 0
            if self.SCORER_DEBUG:
                print "\t\tt=" + t + ", f=" + field
                print "\t\t\tDoc:  n(t,f)=" + str(doc_term_freq) + "\t|f|=" + str(len_d_f)
                print "\t\t\tColl: n(t,f)=" + str(coll_term_freq) + "\t|f|=" + str(len_C_f)
            p_t_theta_d_f[t] = ((1 - self.smoothing_param) * p_t_d_f) + (self.smoothing_param * p_t_C_f)
        return p_t_theta_d_f

    

    def score_doc(self, doc_id, lucene_doc_id=None):
        """ LM score for the given query and document field. """
        if lucene_doc_id is None:
            lucene_doc_id = self.lucene.get_lucene_document_id(doc_id)
        field = self.params.get('field', Lucene.FIELDNAME_CONTENTS)
        if self.SCORER_DEBUG:
            print "Scoring doc ID=" + doc_id

        p_t_theta_d = self.get_term_probs(lucene_doc_id, field)
        # p(q|theta_d) = prod(p(t|theta_d)) ; we return log(p(q|theta_d))
        p_q_theta_d = 0
        for t in self.query_terms:
            # Skips the term if it is not in the field collection
            if p_t_theta_d[t] == 0:
                continue
            if self.SCORER_DEBUG:
                print "\t\tP(" + t + "|" + field + ") = " + str(p_t_theta_d[t])
            p_q_theta_d += math.log(p_t_theta_d[t])
        if self.SCORER_DEBUG:
            print "\tP(d|q)=" + str(p_q_theta_d)
        return p_q_theta_d

        # def score_doc_old(self, doc_id, lucene_doc_id=None):
        # """Entity is matched against the ID field in the index.
        #
        #     The score is zero if either the entity ID is invalid or it does
        #     not contain any of the query term.
        #     """
        #     field = self.params.get('field', Lucene.FIELDNAME_CONTENTS)
        #     # "normal" query
        #     normal_query = self.lucene.get_lucene_query(self.query, field)
        #     # query for the ID field
        #     id_query = self.lucene.get_id_lookup_query(doc_id)
        #     # create Boolean query (normal_query AND id_query)
        #     and_query = self.lucene.get_and_query([normal_query, id_query])
        #
        #     # we only need the top document (and there should only be one)
        #     topdoc = self.lucene.searcher.search(and_query, 1).scoreDocs
        #
        #     if len(topdoc) == 0:
        #         return 0
        #
        #     return topdoc[0].score


class ScorerMLM(ScorerLM):
    """MLM scorer."""

    def __init__(self, lucene, query, params):
        super(ScorerMLM, self).__init__(lucene, query, params)

    # p(f|t) = p(t|C_f) * p(f)/ Sigma(p(t|C_f') * p(f'))
    """
    A mapping function (term to field)
    
    """
    def mapping_f_t(self,term,field):
        field_weights = self.params['field_weights']
        
        sum_prob_t_C_f = 0 # Sigma[p(t|C_f)]
        for eachfield in field_weights:
            len_C_f = self.lucene.get_coll_length(eachfield)
            coll_term_freq = self.lucene.get_coll_termfreq(term, eachfield)
            p_t_C_f = coll_term_freq / len_C_f if len_C_f > 0 else 0 # P(t|C_f) = n(t,C_f) / |C_f|
            sum_prob_t_C_f += p_t_C_f #* field_weights[eachfield]  # METHOD 3
            if field == eachfield:
                term_coll_prob = p_t_C_f #* field_weights[eachfield]

        p = float(term_coll_prob / sum_prob_t_C_f) if sum_prob_t_C_f != 0 else 0
        #print "P(%s|%s)= %s"%(field,term,p)
        return p 
        

    def score_doc(self, doc_id, lucene_doc_id=None):
        """ Scores a given entity using the Mixture of Language Models (using JM smoothing)"""
        if lucene_doc_id is None:
            lucene_doc_id = self.lucene.get_lucene_document_id(doc_id)

        weights = self.params['field_weights']

        # gets term prob for each field
        field_term_probs = {}
        for field in weights.keys():
            field_term_probs[field] = self.get_term_probs(lucene_doc_id, field)

        if self.SCORER_DEBUG:
            print "Scoring doc ID=" + doc_id

        # p(q|theta_d) = prod(p(t|theta_d)) ; we return log(p(q|theta_d))
        p_q_theta_d = 0
        for t in self.query_terms:
            if self.SCORER_DEBUG:
                print "\tt=" + t
            # p(t|theta_d) = sum(mu_f * p(t|theta_d_f))
            p_t_theta_d = 0
            for f in weights:#.iteritems():
                #p_t_theta_d += weights[f] * field_term_probs[f][t] ## METHOD 1
                p_t_theta_d += self.mapping_f_t(t,f) * field_term_probs[f][t] ## METHODS 2,3
                if self.SCORER_DEBUG:
                    print "\t\t\tf=" + f + ", mu_f=" + str(self.mapping_f_t(t,f)) + "  P(t|theta_d,f)=" + str(field_term_probs[f][t])
            # Skips the term if it is not in the field collection
            if p_t_theta_d == 0:
                continue
            p_q_theta_d += math.log(p_t_theta_d)
            if self.SCORER_DEBUG:
                print "\t\tP(t|theta_d)=" + str(p_t_theta_d)
        return p_q_theta_d

      

if __name__ == '__main__':
    pass
