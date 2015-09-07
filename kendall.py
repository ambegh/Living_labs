"""this module calculates kendal coefficient 
   Reads the sets of number from files
"""

def load_ranking(file):
	rank = {}
	for line in open(file,"r"):
		qid,_,pid,r,_,_= line.split()
		if qid in rank :
			rank[qid].append({pid:r})
		else:
			rank[qid] = [{pid:r}]
			
	return rank			

def number_pairs(n):
	return n*(n-1)/2 if n > 1 else 0 

def average(lista):
	return sum(lista) / float(len(lista))

def numerator(rankA , rankB):
	numer = 0 
	N = len(rankA)
	for i in range(0,N):
		for j in range(i+1,N):
			sign =(rankB[i] - rankB[j]) * (rankA[i] - rankA[j])
			if sign > 0: 
			 	numer += 1
			else: 
			 	numer -= 1
	
	return numer 

def kendel_tau(listA,listB):
	numera = numerator(listA,listB)
	den = number_pairs(len(listA))
	return numera/float(den) if den != 0 else 1
		

def main():
	m1 = load_ranking("method1.txt")
	m2 = load_ranking("method2.txt")
	print 'Loaded ranking ...'
	listA = [] 
	listB = []
	accum_kend = []

	for qid in m1:
		for pair in m1[qid]:
			pid = pair.keys()[0]
			rank1 = pair.values()[0]
			rank2 = next((r[pid] for r in m2[qid] if pid in r),0)
			listA.append(int(rank1))
			listB.append(int(rank2))
		t = kendel_tau(listA,listB)
		accum_kend.append(t)
		listA =[]
		listB=[]
	
	print average(accum_kend)
	

if __name__ == '__main__':
	main()
