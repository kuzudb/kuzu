#include "include/query_result.h"

using namespace std;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

bool QueryResult::hasNext() {
    assert(querySummary->getIsExplain() == false);
    return iterator->hasNextFlatTuple();
}

unique_ptr<FlatTuple> QueryResult::getNext() {
    auto tuple = make_unique<FlatTuple>(header->columnDataTypes);
    iterator->getNextFlatTuple(*tuple);
    return tuple;
}

} // namespace main
} // namespace graphflow
