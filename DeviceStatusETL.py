from pyspark import SparkContext
import sys
def distanceSquared(p1, p2):
    return ((p2[0] - p1[0])**2 + (p2[1]-p1[1])**2)
def closestPoint(p, centers):
    bestIndex = 0
    index = 0
    closest = float("+inf")
    for i in range(len(centers)):
        dist = distanceSquared(p, centers[i])
        if dist < closest:
            closest = dist
            bestIndex = i
    return bestIndex
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: hw4.py <input> <output>"
        exit(-1)
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.map(lambda line: line.split(',')) \
        .filter(lambda filt_line: len(filt_line) == 5) \
        .map(lambda fields: (float(fields[3]), float(fields[4]))) \
        .filter(lambda filt: filt[0] != 0.0 and filt[1] != 0.0) \
        .persist()
    
    convergeDist = 0.1
    tempDist = 1.0
    K = 5
    kPoints = rdd.takeSample(False, K, 34)
    while tempDist > convergeDist:
        closest = rdd.map(lambda p: (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(lambda x,y: ((x[0][0] + y[0][0], x[0][1] +
y[0][1]), x[1] + y[1]))
        newPoints = pointStats.map(lambda av: (av[0], (av[1][0][0] / av[1][1], 
av[1][0][1] / av[1][1]))) \
                    .sortByKey().collect()
        tempDist = 0.0
        for i in range(K):
         tempDist += ((kPoints[i][0] - newPoints[i][1][0])**2 + (kPoints[i][1]-
newPoints[i][1][1])**2)
            
        for j in range(K):
         kPoints[j] = newPoints[j][1]
    final_RDD = sc.parallelize(newPoints) \
                .map(lambda field: (field[1][0], field[1][1])) 
    
    final_RDD.repartition(1).saveAsTextFile(sys.argv[2])

sc.stop()
