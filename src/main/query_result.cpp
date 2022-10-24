#include "include/query_result.h"

using namespace std;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

QueryResultHeader::QueryResultHeader(expression_vector expressions) {
    for (auto& expression : expressions) {
        columnDataTypes.push_back(expression->getDataType());
        columnNames.push_back(expression->getRawName());
    }
}

bool QueryResult::hasNext() {
    validateQuerySucceed();
    assert(querySummary->getIsExplain() == false);
    return iterator->hasNextFlatTuple();
}

shared_ptr<FlatTuple> QueryResult::getNext() {
    if (!hasNext()) {
        throw RuntimeException(
            "No more tuples in QueryResult, Please check hasNext() before calling getNext().");
    }
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
