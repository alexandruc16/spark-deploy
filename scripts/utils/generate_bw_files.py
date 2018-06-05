A = [497, 286, 359, 226, 135, 489, 244, 116, 218, 290, 617, 356, 63, 115, 255, 440]
B = [754, 501, 801, 924, 623, 557, 916, 635, 957, 336, 584, 409, 443, 654, 676, 758]
C = [692, 894, 729, 921, 870, 855, 940, 910, 904, 939, 938, 930, 846, 792, 940, 680]
D = [243, 117, 292, 117, 190, 169, 189, 184, 167, 138, 282, 168, 166, 130, 281, 137]
E = [844, 809, 852, 791, 797, 834, 822, 856, 853, 852, 848, 830, 858, 858, 856, 853]
F = [690, 846, 853, 814, 745, 741, 736, 791, 887, 800, 301, 789, 490, 703, 900, 770]
G = [604, 556, 576, 417, 534, 533, 400, 613, 535, 487, 538, 584, 614, 602, 593, 480]
H = [436, 332, 531, 795, 526, 461, 624, 260, 251, 355, 953, 466, 474, 552, 927, 679]


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
