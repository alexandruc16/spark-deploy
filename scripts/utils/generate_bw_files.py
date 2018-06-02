A = [617, 200, 489, 115, 497, 440, 290, 286]
B = [676, 957, 754, 336, 409, 916, 801, 924]
C = [940, 921, 910, 938, 729, 792, 692, 939]
D = [292, 189, 281, 243, 169, 117, 282, 137]
E = [852, 822, 852, 858, 858, 809, 856, 856]
F = [789, 887, 900, 853, 301, 703, 800, 900]
G = [613, 602, 556, 614, 576, 575, 400, 614]
H = [552, 260, 953, 795, 355, 927, 624, 552]


def get_workers():
    rewsult = []
    
    with open('/usr/local/spark/conf/slaves') as f:
        result = [line.rstrip('\n') for line in f]
        
    return result


def generate_bw_files(workers, bw, filename):
    num_workers = len(workers)
    f = open(filename, 'w')
    
    for i in range(0, num_workers):
        for j in range(0, num_workers):
            if i == j:
                continue
            val = "%s:%s:%dkbit" % (workers[i], workers[j], min(bw[i], bw[j]))
            print >> f, val
            
    f.close()
            
            
def main():
    workers = get_workers()
    generate_bw_files(workers, A, "/opt/spark-deploy/scripts/utils/A.txt")
    generate_bw_files(workers, B, "/opt/spark-deploy/scripts/utils/B.txt")
    generate_bw_files(workers, C, "/opt/spark-deploy/scripts/utils/C.txt")
    generate_bw_files(workers, D, "/opt/spark-deploy/scripts/utils/D.txt")
    generate_bw_files(workers, E, "/opt/spark-deploy/scripts/utils/E.txt")
    generate_bw_files(workers, F, "/opt/spark-deploy/scripts/utils/F.txt")
    generate_bw_files(workers, G, "/opt/spark-deploy/scripts/utils/G.txt")
    generate_bw_files(workers, H, "/opt/spark-deploy/scripts/utils/H.txt")
    
    

if __name__ == "__main__":
    main()
