#include "include/query_result.h"

using namespace std;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

bool QueryResult::hasNext() {
    validateQuerySucceed();
    assert(querySummary->getIsExplain() == false);
    return iterator->hasNextFlatTuple();
}

unique_ptr<FlatTuple> QueryResult::getNext() {
    validateQuerySucceed();
    auto tuple = make_unique<FlatTuple>(header->columnDataTypes);
    iterator->getNextFlatTuple(*tuple);
    return tuple;
}

void QueryResult::validateQuerySucceed() {
    if (!success) {
        throw Exception(errMsg);
    }
}

} // namespace main
} // namespace graphflow
