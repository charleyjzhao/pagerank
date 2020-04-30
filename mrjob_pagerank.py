# Template for writing MapReduce programs using mrjob
# % python mrjob-pagerank.py <input file>  -q

from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
orig_stdout = sys.stdout  # stash sys.stdout before it is redefined

class MR_program(MRJob):

    def configure_args(self):
        super(MR_program, self).configure_args()
        self.add_passthru_arg('--nodes')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('--beta')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('-N')    ##  <--- specify which new command line args are used

    def mapper_1(self, _, line):
        num_nodes = int(self.options.nodes)
        node_prob = 1/num_nodes
        curr, *neighbors = line.split()
        yield (curr, neighbors)
        for neigh in neighbors:
            yield (neigh, node_prob/len(neighbors))
        

    def reducer_1(self, x, pr_y_by_n_or_nbrs_of_x):  
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        lst = list(pr_y_by_n_or_nbrs_of_x)
        to_sum = []
        zs = []
        for item in lst:
            if isinstance(item, float):
                to_sum.append(item)
            else:
                zs = item
        pr_x = (1-beta)/num_nodes + beta*sum(to_sum)
        for z in zs:
            yield (z, pr_x/len(zs))
        yield (x, zs)
        print(">>>> " + str(x), pr_x, file=orig_stdout)

    def reducer_2(self, x, pr_y_by_n_or_nbrs_of_x):  
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        lst = list(pr_y_by_n_or_nbrs_of_x)
        to_sum = []
        zs = []
        for item in lst:
            if isinstance(item, float):
                to_sum.append(item)
            else:
                zs = item
        pr_x = (1-beta)/num_nodes + beta*sum(to_sum)
        yield x, pr_x
        

    def steps(self):
        N = int(self.options.N)
        return [MRStep(mapper=self.mapper_1)] + \
               [MRStep(reducer=self.reducer_1)]*N + \
               [MRStep(reducer=self.reducer_2)]
               

if __name__ == '__main__':
    # change to match the name of the class
    MR_program.run()
    
