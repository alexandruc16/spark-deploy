import argparse
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta


BANDWIDTH_LIMITS = {
    'A' : [440, 497, 489, 617, 290, 286, 359, 356, 244, 255, 218, 226, 63, 135, 116, 115],
    'B' : [916, 957, 924, 801, 754, 676, 758, 654, 584, 635, 557, 623, 443, 501, 409, 336],
    'C' : [938, 940, 939, 940, 921, 910, 930, 904, 855, 846, 894, 870, 680, 692, 729, 792],
    'D' : [243, 282, 281, 292, 189, 169, 184, 190, 166, 168, 167, 138, 117, 130, 137, 117],
    'E' : [868, 868, 856, 856, 852, 853, 852, 853, 854, 848, 844, 830, 797, 791, 809, 822],
    'F' : [900, 846, 887, 853, 791, 800, 789, 814, 741, 736, 770, 745, 690, 490, 703, 301],
    'G' : [602, 604, 614, 613, 576, 584, 556, 593, 535, 538, 533, 534, 417, 480, 400, 487],
    'H' : [953, 927, 679, 795, 552, 624, 531, 526, 436, 474, 466, 461, 332, 251, 355, 260]
}


class BenchmarkResults:
    def __init__(self, data, start, end):
        self.data = data
        self.start = start
        self.end = end


def generate_hibench_data(folder):
    hibench_results = [f for f in os.listdir(folder) if f.endswith(".report")]
    results = dict()
    
    for filename in hibench_results:
        path = os.path.join(folder, filename)
        benchmarks = dict()
        bandwidth_setup = os.path.splitext(os.path.basename(filename))[0]
        
        with open(path) as f:          
            next(f)
            lines_in_file = 1
            start = ''
            end = ''
             
            for line in f:
                data = line.rstrip('\n').split()
                benchmark_name = data[0]
                measurement = float(data[4])
                
                if benchmark_name in benchmarks:
                    benchmarks[benchmark_name].append(measurement)
                else:
                    benchmarks[benchmark_name] = [measurement]
                
                lines_in_file += 1
                
                end = "%s %s" % (data[1], data[2])
                
                if lines_in_file == 2:
                    start = end
            
            startDatetime = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            endDatetime = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            results[bandwidth_setup] = BenchmarkResults(benchmarks, startDatetime, endDatetime)
    
    return results
    
    
def sort_experiments(configs):
    result = ['no_limit']
    
    if "no_limit" in configs:
        configs.remove('no_limit')
    
    result.extend(sorted(configs))

    return result
    
    
def plot_hibench_results(data, folder):
    benchmarks = []
    
    bandwidth_configurations = sort_experiments(data.keys())
    
    for results in data.values():
        values = results.data
        
        for benchmark_name in values.keys():
            if benchmark_name not in benchmarks:
                benchmarks.append(benchmark_name)
                
    for benchmark in benchmarks:
        values = []
        
        for bandwidth_configuration in bandwidth_configurations:
            if bandwidth_configuration in data and benchmark in data[bandwidth_configuration].data:
                values.append(data[bandwidth_configuration].data[benchmark])
            
        fig = plt.figure()
        plt.boxplot(values, showmeans=True)
        plt.xticks(range(1, len(values) + 1), bandwidth_configurations)
        plt.ylim(ymin = 0)
        plt.ylabel('makespan (s)')
        plt.savefig(os.path.join(folder, benchmark + '_report.png'))
        plt.close(fig)
        
        
def get_vary_files_per_node(folder):
    reports = []
    node_folders = sorted(os.listdir(folder))
    
    for i in range(0, len(node_folders)):
        files = dict()
        folder_path = os.path.join(folder, node_folders[i])
        filenames = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f == "vary.out"]
        
        for f in filenames:
            last_modif_date = datetime.fromtimestamp(os.path.getmtime(f))
            files[f] = last_modif_date
        
        reports.append(files)
        
    return reports

    
def plot_bandwidth_usage(config, start, end, node_files):
    for i in range(0, len(node_files)):
        limit = 0
        
        if config in BANDWIDTH_LIMITS:
            limit = BANDWIDTH_LIMITS[config][i] / 8
        
        closest_file_lastmod = datetime.max
        values = []
        
        for fnam, fmod in node_files[i].items():
            if fmod > end and fmod < closest_file_lastmod:
                values = []
                closest_file_lastmod = fmod
                fstart = fmod
                
                for line in reversed(list(open(fnam))):
                    fstart = fstart - timedelta(seconds=1)
                    
                    if fstart > end:
                        continue
                
                    values.insert(0, line)
                    
                    if fstart < start:
                        break
                        
            num_values = len(values)
            x_values = range(0, num_values)
            limits = [limit] * num_values
            fig_name = os.path.join(os.path.dirname(fnam), config + '_bw.png')
            
            fig = plt.figure()
            plt.plot(x_values, values, 'b')
            
            if limit != 0:
                plt.plot(x_values, limits, 'r')
            
            plt.xlabel('time (s)')
            plt.ylabel('bandwidth (Mbps)')
            plt.savefig(fig_name)
            plt.close(fig)
        

def plot_hibench_bandwidths(benchmark_results, folder):
    reports = []
    node_folders = sorted(os.listdir(folder))
    
    for i in range(0, len(node_folders)):
        for k, v in benchmark_results.items():
            folder_name = os.path.join(folder, node_folders[i])
            out_bw_filename = os.path.join(folder_name, "monitor_%s.out" % k)
            bw_limit_filename = os.path.join(folder_name, "limits_%s.out" % k)
            lims_lastmod = datetime.fromtimestamp(os.path.getmtime(out_bw_filename))
            bws_lastmod = datetime.fromtimestamp(os.path.getmtime(bw_limit_filename))
            bws = []
            lims = []
            
            with open(out_bw_filename, 'r') as bw, open(bw_limit_filename, 'r') as lim:
                out_bw = list(reversed(bw.readlines()))
                bw_limit = list(reversed(lim.readlines()))
                bw_len = len(out_bw)
                lim_len = len(bw_limit)
                last_lim = 0
                
                for j in range(0, bw_len):
                    bws.insert(0, float(out_bw[j]))
                    lims.insert(0, bw_limit[last_lim])
                    
                    bws_lastmod = bws_lastmod - timedelta(seconds=1)
                    
                    if bws_lastmod < lims_lastmod and last_lim < lim_len:
                        lims_lastmod = lims_lastmod - timedelta(seconds=10)
                        last_lim += 1
                    
            
            fig_name = os.path.join(folder_name, k + '_bw.png')
            
            fig = plt.figure()
            plt.plot(range(0, len(bws)), bws, 'b')
            plt.plot(range(0, len(lims)), lims, 'r')
            plt.xlabel('time (s)')
            plt.ylabel('bandwidth (MB/s)')
            plt.savefig(fig_name)
            plt.close(fig)
        
    return reports


def main():
    parser = argparse.ArgumentParser(description="Generate plots from benchmark "
                                     "results.", epilog = "Example Usage: "
                                     "python plot_reports.py "
                                     "<path-to-results-directory>")

    parser.add_argument("results_directory",
                        action="store",
                        help="Path to directory containing benchmark results.")
                        
    args = parser.parse_args()
    master_folder = os.path.join(args.results_directory, 'master')
    hibench_results = generate_hibench_data(master_folder)
    
    plot_hibench_results(hibench_results, master_folder)
    plot_hibench_bandwidths(hibench_results, args.results_directory)
    

if __name__ == "__main__":
    main()
