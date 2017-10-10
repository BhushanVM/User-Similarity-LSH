from pyspark import SparkContext
from collections import defaultdict
from operator import add
from itertools import combinations
import sys
import timeit
start = timeit.default_timer()

sc = SparkContext(appName="inf553")

inputFile = open(sys.argv[1],"r")

def makeSets(iterator):
	globaldict = {}
	for line in iterator:
		line = line.split(",",1)
		values = line[1].strip().split(",")

		for i in range(0, len(values)):
		if globaldict.has_key(int(line[0].strip("U"))):
				s = globaldict.get(int(line[0].strip("U")))
				s.add(int(values[i]))
				globaldict[int(line[0].strip("U"))] = s
			else:
				s = set()
				s.add(int(values[i]))
				globaldict[int(line[0].strip("U"))] = s

	return globaldict.items()

def makePairs(iterator):
	inputList = []
	for line in iterator:
		line = line.split(",",1)
		values = line[1].strip().split(",")
		for i in range(0, len(values)):
			inputList.append("(" + line[0].strip("U") + "," + values[i] + ")")

	return inputList


def findSignature(x):
	signature = []
	inputList = list(x)

	for i in range(0,20):
		row = [sys.maxint]*count
		for j in range(0,len(inputList)):
			temp = inputList[j].strip("()").split(",")
			h = int(temp[1]) #h=row no
			u = int(temp[0])-1 #u = user no
			#print  + ": U"+str((3*h+13*i)%100)
			if row[u] > ((3*h+13*i)%100):
				row[u] = ((3*h+13*i)%100)

		signature.append(row)
	return signature

def computeKeyPairs(x):
	signList = list(x)
	dict = {}
	for i in range(0, len(signList[0])):
		temp = ""
		for j in range(0, len(signList)):
			temp += "," + str(signList[j][i])
			# print signList[j][i]
		temp = temp[1:]
		# print temp
		if dict.has_key(temp):
			dict[temp] += "," + str(i)
		else:
			dict[temp] = str(i)
	#Hash Dictionary of each column made - stored in dict

	keyPairs = []
	for item in dict:
		if len(dict[item].split(",")) > 1:
			x = combinations(map(int, dict[item].split(",")), 2)
			for i in x:
				# print i
				keyPairs.append(i)

	return keyPairs
	#Making pair of similar users done out of dict

def findTopFive(keyPairs):
	outputDict = {}
	for i in keyPairs:
		temp = i
		#print temp
		jaccardVal = float(len(globaldict.get(temp[0] + 1).intersection(globaldict.get(temp[1] + 1)))) / float(
			len(globaldict.get(temp[0] + 1).union(globaldict.get(temp[1] + 1))))

		if outputDict.has_key(temp[0] + 1):
			tempDict = outputDict.get(temp[0] + 1)
			#For limiting to 5
			if len(tempDict) == 5:
				minVal = min(tempDict.itervalues())
				# print minVal
				if jaccardVal > minVal:
					# print "in1"
					lKey = [key for key, value in tempDict.iteritems() if value == minVal]
					lKey = max(lKey)
					tempDict.pop(lKey)
					tempDict[temp[1] + 1] = jaccardVal
					outputDict[temp[0] + 1] = tempDict
				elif jaccardVal == minVal:
					# print "in2"
					lKey = [key for key, value in tempDict.iteritems() if value == minVal]
					lKey = max(lKey)
					if temp[1] + 1 < lKey:
						tempDict.pop(lKey)
						tempDict[temp[1] + 1] = jaccardVal
					outputDict[temp[0] + 1] = tempDict
			else:
				# print "in3"
				tempDict[temp[1] + 1] = jaccardVal
				outputDict[temp[0] + 1] = tempDict
		else:
			tempDict = {}
			tempDict[temp[1] + 1] = jaccardVal
			outputDict[temp[0] + 1] = tempDict

		if outputDict.has_key(temp[1] + 1):
			tempDict = outputDict.get(temp[1] + 1)
			# For limiting to 5
			if len(tempDict) == 5:
				minVal = min(tempDict.itervalues())
				# print minVal
				if jaccardVal > minVal:
					# print "in1"
					lKey = [key for key, value in tempDict.iteritems() if value == minVal]
					lKey = max(lKey)
					tempDict.pop(lKey)
					tempDict[temp[0] + 1] = jaccardVal
					outputDict[temp[1] + 1] = tempDict
				elif jaccardVal == minVal:
					# print "in2"
					lKey = [key for key, value in tempDict.iteritems() if value == minVal]
					lKey = max(lKey)
					if temp[0] + 1 < lKey:
						tempDict.pop(lKey)
						tempDict[temp[0] + 1] = jaccardVal
					outputDict[temp[1] + 1] = tempDict
			else:
				# print "in3"
				tempDict[temp[0] + 1] = jaccardVal
				outputDict[temp[1] + 1] = tempDict
		else:
			tempDict = {}
			tempDict[temp[0] + 1] = jaccardVal
			outputDict[temp[1] + 1] = tempDict

	return outputDict.items()




def pri(x):
   temp = list(x)
   print(len(temp))
   return temp


def combineDict(x):
	inpList = list(x)
	#print inpList

	dict = {}
	for i in range(0,len(inpList)):
		temp = inpList[i]

		for pairKey in temp.iterkeys():
			if not dict.has_key(pairKey):
				if not len(dict)==5:
					dict[pairKey] = temp[pairKey]
				else:
					minVal = min(dict.itervalues())

					if temp[pairKey] > minVal:
						lKey = [key for key, value in dict.iteritems() if value == minVal]
						lKey = max(lKey)
						dict.pop(lKey)
						dict[pairKey] = temp[pairKey]
					elif temp[pairKey] == minVal:
						lKey = [key for key, value in dict.iteritems() if value == minVal]
						lKey = max(lKey)
						if lKey > pairKey:
							dict.pop(lKey)
							dict[pairKey] = temp[pairKey]
							#print dict

	#print dict
	return dict

def main():
	inpRDD = sc.parallelize(inputFile,4)
	global count
	count = inpRDD.count()
	#Count printing successful.

	dictList = inpRDD.mapPartitions(makeSets).collect()
	global globaldict
	globaldict = {x[0]:x[1] for x in dictList}

	pairsRDD = inpRDD.mapPartitions(makePairs)
	pairsList = pairsRDD.collect()

	signRDD = sc.parallelize(pairsList,1)

	matchRDD = signRDD.mapPartitions(findSignature)
	similarRDD = sc.parallelize(matchRDD.collect(),5).mapPartitions(computeKeyPairs).distinct().mapPartitions(findTopFive).groupByKey()
	resultList = similarRDD.mapValues(combineDict).collect()
	outputDict = {x[0]:x[1] for x in resultList}



	#FileWriting
	output = sys.argv[2]
	file = open(output,'w')

	#print outputDict
	for i in sorted(outputDict):
		output = "U"+str(i)+":"
		similarSet = outputDict.get(i)
		similarSet = sorted(similarSet.items())
		for j in range(0,len(similarSet)):
			output += "U"+str(similarSet[j][0])+","
		file.write(output[:len(output)-1]+"\n")



	#Timer
	stop = timeit.default_timer()
	print(stop - start)

if __name__ == "__main__":
	main()