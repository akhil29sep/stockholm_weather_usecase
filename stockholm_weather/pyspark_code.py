fin = open("barometer_1756_1858.txt", "rt")
fout = open("barometer_1756_1858_1.txt", "wt")

for line in fin:
    fout.write(','.join(line.split()))

fin.close()
fout.close()