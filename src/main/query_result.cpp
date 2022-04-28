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

shared_ptr<FlatTuple> QueryResult::getNext() {
    validateQuerySucceed();
    return iterator->getNextFlatTuple();
}

void QueryResult::validateQuerySucceed() {
    if (!success) {
        throw Exception(errMsg);
    }
}

} // namespace main
} // namespace graphflow
