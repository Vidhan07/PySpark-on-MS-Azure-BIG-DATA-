import sys
import findspark

findspark.init()
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("App")
    sc = SparkContext(conf=conf)
    # read input text file to RDD
    lines = sc.textFile("New_Text.txt")  #input to the application
    pos_value = sc.textFile("positive.txt") #input to the application
    neg_value = sc.textFile("negative.txt") #input to the application
    output = open('output.txt', 'w') #output to the application
    output.write('')
    output.close()
    output = open('output.txt', 'a')

    # collect the RDD to a list
    list = lines.collect()
    list1 = pos_value.collect()
    list2 = neg_value.collect()
    dict_pos = {}
    dict_neg = {}
    product = {}
    count = 0
    pos = 0
    neg = 0
    for l in list:
        words = l.strip().split()
        for word in words:
            if word in list1:
                if list1.index(word) > 34:
                    pos += 1
            elif word in list2:
                if list2.index(word) > 34:
                    neg += 1
        if words[0] not in dict_pos:
            dict_pos[words[0]] = 0
        if words[0] not in dict_neg:
            dict_neg[words[0]] = 0

        dict_pos[words[0]] += pos
        dict_neg[words[0]] += neg

        product[words[0]] = 'Positive' if dict_pos[words[0]] > dict_neg[words[0]] else 'Negative'
        pos = 0
        neg = 0
    for a in product:
        print(a, product[a])
        output.write(a + ' ' + product[a] + '\n')
    output.close()