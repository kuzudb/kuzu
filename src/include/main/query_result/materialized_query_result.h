#pragma once

#include "main/query_result.h"

namespace kuzu {
namespace processor {
class FactorizedTable;
}

namespace main {

class MaterializedQueryResult : public QueryResult {
public:
    MaterializedQueryResult();

private:
    std::shared_ptr<processor::FactorizedTable> factorizedTable;
    std::unique_ptr<processor::FlatTupleIterator> iterator;
    std::shared_ptr<processor::FlatTuple> tuple;
};

}
}
