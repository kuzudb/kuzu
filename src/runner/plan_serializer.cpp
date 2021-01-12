#include <fstream>
#include <iostream>
#include <thread>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/logical/extend/logical_extend.h"
#include "src/processor/include/operator/logical/property_reader/logical_node_property_reader.h"
#include "src/processor/include/operator/logical/property_reader/logical_rel_property_reader.h"
#include "src/processor/include/operator/logical/scan/logical_scan.h"
#include "src/processor/include/plan/logical/logical_plan.h"
#include "src/processor/include/processor.h"

using namespace graphflow::processor;
using namespace graphflow::common;

using namespace std;

unique_ptr<LogicalPlan> create_plan() {
    return make_unique<LogicalPlan>(
        // make_unique<LogicalNodePropertyReader>("a", "comment", "creationDate",
        //    make_unique<LogicalNodePropertyReader>("a", "comment", "length",
        //        make_unique<LogicalNodePropertyReader>("a", "comment", "cid",
        //            make_unique<LogicalNodePropertyReader>("a", "comment", "browserUsed",
        make_unique<LogicalNodePropertyReader>(
            "a", "person", "fName", make_unique<LogicalScan>("a", "person")));
}

int main(int argc, char* argv[]) {
    auto graph = make_unique<Graph>("/home/p43gupta/extended/datasets/ldbc/0.1/ser/", 40960);
    auto plan = create_plan();
    auto physicalPlan = /*deserPlan*/ plan->mapToPhysical(*graph);
    uint64_t numThreads = 8;
    auto processor = make_unique<QueryProcessor>(numThreads);
    auto result = processor->execute(physicalPlan, *graph, 1 /*maxNumThreads*/);
    std::cout << "# output tuples: " << result->first.getNumOutputTuples() << std::endl;
    std::cout << "run time (ms): " << result->second.count() << " ms" << std::endl;
    // plan->serialize("...");
}
