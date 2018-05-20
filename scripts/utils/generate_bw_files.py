A = [440, 497, 489, 617, 290, 286, 359, 356, 244, 255, 218, 226, 63, 135, 116, 115]
B = [916, 957, 924, 801, 754, 676, 758, 654, 584, 635, 557, 623, 443, 501, 409, 336]
C = [938, 940, 939, 940, 921, 910, 930, 904, 855, 846, 894, 870, 680, 692, 729, 792]
D = [243, 282, 281, 292, 189, 169, 184, 190, 166, 168, 167, 138, 117, 130, 137, 117]
E = [868, 868, 856, 856, 852, 853, 852, 853, 854, 848, 844, 830, 797, 791, 809, 822]
F = [900, 846, 887, 853, 791, 800, 789, 814, 741, 736, 770, 745, 690, 490, 703, 301]
G = [602, 604, 614, 613, 576, 584, 556, 593, 535, 538, 533, 534, 417, 480, 400, 487]
H = [953, 927, 679, 795, 552, 624, 531, 526, 436, 474, 466, 461, 332, 251, 355, 260]


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
            val = "%s:%s:%d" % (workers[i], workers[j], min(bw[i], bw[j]))
            print >> f, val
            
    f.close()
            
            
def main():
    workers = get_workers()
    generate_bw_files(workers, A, "A.txt")
    generate_bw_files(workers, B, "B.txt")
    generate_bw_files(workers, C, "C.txt")
    generate_bw_files(workers, D, "D.txt")
    generate_bw_files(workers, E, "E.txt")
    generate_bw_files(workers, F, "F.txt")
    generate_bw_files(workers, G, "G.txt")
    generate_bw_files(workers, H, "H.txt")
    
    

if __name__ == "__main__":
    main()
