#include "include/query_result.h"

using namespace std;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

QueryResult::QueryResult(unique_ptr<QueryResultHeader> header,
    shared_ptr<FactorizedTable> factorizedTable, unique_ptr<QuerySummary> querySummary)
    : header{move(header)}, factorizedTable{move(factorizedTable)}, querySummary{
                                                                        move(querySummary)} {
    iterator = make_unique<FlatTupleIterator>(*this->factorizedTable);
}

QueryResult::QueryResult(unique_ptr<QuerySummary> querySummary) : querySummary{move(querySummary)} {
    assert(this->querySummary->getIsExplain() == true);
}

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
