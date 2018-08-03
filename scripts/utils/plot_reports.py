import argparse
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta


BANDWIDTH_CONFIGURATIONS = ['no_limit', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
ITERATIONS_PER_TEST = 10

        
def generate_hibench_data(folder):
    hibench_results = [f for f in os.listdir(folder) if f.endswith(".report")]
    results = dict()
    
    for filename in hibench_results:
        path = os.path.join(folder, filename)
        benchmark_name = os.path.splitext(os.path.basename(filename))[0]
        results[benchmark_name] = dict()
        
        with open(path) as f:
            next(f)
            i = 0 # count experiment iterations
            measurements = []
            
            for line in f:
                raw_data = line.rstrip('\n').split()
                measurements.append(float(raw_data[4]))
                
                if (i + 1) % ITERATIONS_PER_TEST == 0:
                    results[benchmark_name][BANDWIDTH_CONFIGURATIONS[i / ITERATIONS_PER_TEST]] = measurements
                    measurements = []
                
                i += 1
                
    return results
        
    
def plot_hibench_results(data, folder):
    for experiment in data.keys():
        values = []
        
        for config in BANDWIDTH_CONFIGURATIONS:
            if config in data[experiment].keys():
                values.append(data[experiment][config])
            
        fig = plt.figure()
        plt.boxplot(values, showmeans=True)
        plt.xticks(range(1, len(BANDWIDTH_CONFIGURATIONS) + 1), BANDWIDTH_CONFIGURATIONS)
        plt.ylim(ymin = 0)
        plt.ylabel('makespan (s)')
        plt.savefig(os.path.join(folder, experiment + '_report.png'))
        plt.close(fig)


def plot_hibench_bandwidths(data, folder):
    for experiment in data.keys():
        values = []
        
        for config in BANDWIDTH_CONFIGURATIONS:
            if config in data[experiment].keys():
                values.append(data[experiment][config])
                
    node_folders = [os.path.join(folder, d) for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]
    
    for i in range(0, len(node_folders)):
        for experiment in data.keys():
            for config in BANDWIDTH_CONFIGURATIONS:
                folder_name = os.path.join(folder, node_folders[i])
                file_id = '%s_%s' % (experiment, config)
                out_bw_filename = os.path.join(folder_name, "monitor_%s.out" % file_id)
                bw_limit_filename = os.path.join(folder_name, "limits_%s.out" % file_id)
                
                with open(out_bw_filename, 'r') as bw:
                    bws = map(float, bw)
                    fig_name = os.path.join(folder_name, file_id + '_bw.png')
                    fig = plt.figure()
                    plt.plot(range(0, len(bws)), bws, 'b')
                    plt.xlabel('time (s)')
                    plt.ylabel('bandwidth (MB/s)')
                    
                    if os.path.isfile(bw_limit_filename):
                        with open(bw_limit_filename, 'r') as lim:
                            lims = map(float, lim)
                            plt.plot(range(0, len(lims)), lims, 'r')
                    
                    plt.savefig(fig_name)
                    plt.close(fig)
                    

def main():
    parser = argparse.ArgumentParser(description="Generate plots from benchmark "
                                     "results.", epilog = "Example Usage: "
                                     "python plot_reports.py "
                                     "<path-to-results-directory>")

    parser.add_argument("results_directory",
                        action="store",
                        help="Path to directory containing benchmark results.")
                        
    args = parser.parse_args()
    #master_folder = os.path.join(args.results_directory, 'master')
    hibench_results = generate_hibench_data(args.results_directory)
    
    plot_hibench_results(hibench_results, args.results_directory)
    plot_hibench_bandwidths(hibench_results, args.results_directory)
    

if __name__ == "__main__":
    main()

