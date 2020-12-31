#include "src/processor/include/operator/column_reader/node_property_column_reader.h"
#include "src/processor/include/operator/list_reader/adj_list_extend.h"
#include "src/processor/include/operator/scan/scan.h"
#include "src/processor/include/operator/sink/sink.h"
#include "src/processor/include/query_plan.h"

using namespace graphflow::processor;

int main(int argc, char* argv[]) {
    auto morsel =
        make_shared<MorselDescSingleLabelNodeIDs>(1 /* label */, 1025012 /* max_offset */);
    auto plan = make_unique<QueryPlan>(
        new Sink(new AdjListExtend("a", "b", Direction::FWD, 0 /*nodeLabel*/, 0 /*relLabel*/,
            new NodePropertyColumnReader(
                "a", 0 /*label*/, "prop" /*propertyName*/, new ScanSingleLabel("a", morsel)))));
}
