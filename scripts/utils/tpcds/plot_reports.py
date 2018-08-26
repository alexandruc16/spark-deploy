import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import os
from datetime import datetime, timedelta


BANDWIDTH_CONFIGURATIONS = ['no_limit', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
ITERATIONS_PER_TEST = 10

        
def get_tpcds_data(folder):
    tpcds_results = [f for f in os.listdir(folder) if f.endswith(".json")]
    result = dict()
    result['by_query'] = dict()
    result['by_config'] = dict()
    
    for filename in tpcds_results:
        bandwidth_config = os.path.splitext(filename)[0]
        path = os.path.join(folder, filename)
        data = []

        if bandwidth_config not in result['by_config']:
            result['by_config'][bandwidth_config] = dict()

        for line in open(path, 'r'):
            data.append(json.loads(line))

        for results in data:
            query = results['name']

            if query not in result['by_query']:
                result['by_query'][query] = dict()

            result['by_query'][query][bandwidth_config] = results['runtimes']
            result['by_config'][bandwidth_config][query] = results['runtimes']
                
    return result
        
    
def plot_tpcds_results(data, folder):
    for query in data['by_query'].keys():
        values = []
        configs = sorted(data['by_query'][query].keys())
        configs.insert(0, configs.pop())  # move no_limit to first position

        for config in configs:
            values.append(data['by_query'][query][config])
            
        fig = plt.figure()
        plt.boxplot(values, showmeans=True)
        plt.xticks(range(1, len(configs) + 1), configs)
        plt.ylim(ymin=0)
        plt.ylabel('makespan (s)')
        plt.savefig(os.path.join(folder, query + '_report.png'))
        plt.close(fig)

    for config in data['by_config'].keys():
        values = []
        queries = sorted(data['by_config'][config].keys())

        for query in queries:
            values.append(data['by_config'][config][query])

        fig = plt.figure()
        plt.boxplot(values, showmeans=True)
        plt.xticks(range(1, len(queries) + 1), queries, rotation=90)
        plt.ylim(ymin=0)
        plt.ylabel('makespan (s)')
        plt.savefig(os.path.join(folder, config + '_report.png'))
        plt.close(fig)


def plot_bw(fig_name, bws, lims=None):
    fig = plt.figure()
    plt.plot(range(0, len(bws)), bws, 'b')
    plt.xlabel('time (s)')
    plt.ylabel('bandwidth (Mbps)')

    if lims is not None and len(lims):
        plt.plot(range(0, len(lims)), lims, 'r')

    plt.savefig(fig_name)
    plt.close(fig)


def plot_tpcds_bandwidths(folder):
    node_folders = [os.path.join(folder, d) for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]

    for i in range(0, len(node_folders)):
        for config in BANDWIDTH_CONFIGURATIONS:
            folder_name = os.path.join(folder, node_folders[i])
            file_id = '%s_%s' % ("tpcds", config)
            out_bw_filename = os.path.join(folder_name, "monitor_%s.out" % file_id)
            in_bw_filename = os.path.join(folder_name, "monitor_%s.in" % file_id)
            bw_limit_filename = os.path.join(folder_name, "limits_%s.out" % file_id)
            lims = []

            with open(out_bw_filename, 'r') as out_bw, open(in_bw_filename, 'r') as in_bw:
                out_bws = map(float, out_bw)
                in_bws = map(float, in_bw)
                out_fig_name = os.path.join(folder_name, file_id + '_bw_out.png')
                in_fig_name = os.path.join(folder_name, file_id + '_bw_in.png')

                if os.path.isfile(bw_limit_filename):
                    with open(bw_limit_filename, 'r') as lim:
                        lims = [x / 1000 for x in map(float, lim)]
                        lims = np.repeat(lims, 5)

                plot_bw(out_fig_name, out_bws, lims)
                plot_bw(in_fig_name, in_bws, lims)


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
    hibench_results = get_tpcds_data(args.results_directory)
    
    #plot_tpcds_results(hibench_results, args.results_directory)
    plot_tpcds_bandwidths(args.results_directory)
    

if __name__ == "__main__":
    main()

