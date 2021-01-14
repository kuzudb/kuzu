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
    // return
    // make_unique<LogicalPlan>(make_unique<LogicalExtend>("a", "person", "b", "organisation",
    //     "studyAt", Direction::FWD, make_unique<LogicalScan>("a", "person")));

    return make_unique<LogicalPlan>(make_unique<LogicalRelPropertyReader>("a", "person", "b",
        "person", "knows", Direction::FWD, "date",
        make_unique<LogicalExtend>("a", "person", "b", "person", "knows", Direction::FWD,
            make_unique<LogicalScan>("a", "person"))));
}

int main(int argc, char* argv[]) {
    cout << "main 0" << endl;
    Graph graph("/home/p43gupta/extended/datasets/ldbc/0.1/ser", 40960);
    cout << "c" << endl;
    auto plan = create_plan();
    cout << "phy s" << endl;
    auto phyPlan = plan->mapToPhysical(graph);
    cout << "phy done" << endl;
    QueryProcessor processor(4);
    cout << "start" << endl;
    auto result = processor.execute(phyPlan, graph, 1);
    cout << "endth" << endl;
    cout << result->first.getNumOutputTuples() << endl;
    cout << result->second.count() << endl;
}
