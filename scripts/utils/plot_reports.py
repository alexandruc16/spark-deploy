import argparse
import matplotlib.pyplot as plt
import os


def generate_hibench_data(folder):
    hibench_results = [f for f in os.listdir(folder) if f.endswith(".report")]
    results = dict()
    
    for filename in hibench_results:
        path = os.path.join(folder, filename)
        benchmarks = dict()
        bandwidth_setup = os.path.splitext(os.path.basename(filename))[0]
        
        with open(path) as f:
            next(f)
            
            for line in f:
                data = line.rstrip('\n').split()
                benchmark_name = data[0]
                measurement = float(data[4])
                
                if benchmark_name in benchmarks:
                    benchmarks[benchmark_name].append(measurement)
                else:
                    benchmarks[benchmark_name] = [measurement]
                    
            results[bandwidth_setup] = benchmarks    
    return results
    
    
def plot_hibench_results(data):
    benchmarks = []
    bandwidth_configurations = sorted(data.keys())
    
    for results in data.values():
        for benchmark_name in results.keys():
            if benchmark_name not in benchmarks:
                benchmarks.append(benchmark_name)
                
    for benchmark in benchmarks:
        values = []
        
        for bandwidth_configuration in bandwidth_configurations:
            values.append(data[bandwidth_configuration][benchmark])
            
        plt.figure()
        plt.boxplot(values, showmeans=True)
        plt.xticks(range(1, len(values) + 1), bandwidth_configurations)
        plt.savefig(benchmark + '.png')
    

def main():
    parser = argparse.ArgumentParser(description="Generate plots from benchmark "
                                     "results.", epilog = "Example Usage: "
                                     "python plot_reports.py "
                                     "<path-to-results-directory>")

    parser.add_argument("results_directory",
                        action="store",
                        help="Path to directory containing benchmark results.")
                        
    args = parser.parse_args()
    hibench_results = generate_hibench_data(args.results_directory)
    plot_hibench_results(hibench_results)


if __name__ == "__main__":
    main()

