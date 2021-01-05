#include <fstream>
#include <iostream>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/column_reader/node_property_column_reader.h"
#include "src/processor/include/operator/list_reader/adj_list_extend.h"
#include "src/processor/include/operator/operator_ser_deser_factory.h"
#include "src/processor/include/operator/scan/scan.h"
#include "src/processor/include/operator/sink/sink.h"
#include "src/processor/include/plan/plan.h"
#include "src/processor/include/task_system/morsel.h"

using namespace graphflow::processor;
using namespace graphflow::common;

using namespace std;

unique_ptr<QueryPlan> create_plan() {
    auto morsel =
        make_shared<MorselDescSingleLabelNodeIDs>(1 /* label */, 1025012 /* max_offset */);
    auto scan = make_unique<ScanSingleLabel>("a", morsel);
    auto nodePropertyColumnReader =
        make_unique<NodePropertyColumnReader>("a", 0 /*label*/, "prop" /*propertyName*/);
    nodePropertyColumnReader->setPrevOperator(move(scan));
    auto adjListExtend =
        make_unique<AdjListExtend>("a", "b", Direction::FWD, 0 /*nodeLabel*/, 0 /*relLabel*/);
    adjListExtend->setPrevOperator(move(nodePropertyColumnReader));
    unique_ptr<Operator> sink = make_unique<Sink>();
    sink->setPrevOperator(move(adjListExtend));
    return make_unique<QueryPlan>(move(sink));
}

int main(int argc, char* argv[]) {

    string fname("...");

    FileSerHelper* fsh = new FileSerHelper(fname);
    auto plan = create_plan();
    plan->serialize(*fsh);
    delete fsh;
    plan.reset();

    // FileDeserHelper* fdsh = new FileDeserHelper(fname);
    // QueryPlan deserQP{*fdsh};
    // delete fdsh;
}
