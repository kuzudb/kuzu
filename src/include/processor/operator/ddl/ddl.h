#pragma once

#include "catalog/catalog.h"
#include "processor/operator/physical_operator.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class DDL : public PhysicalOperator {

public:
    DDL(Catalog* catalog, uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, catalog{catalog} {}

    virtual string execute() = 0;

    bool getNextTuples() override { assert(false); }

    virtual ~DDL() = default;

protected:
    Catalog* catalog;
};

} // namespace processor
} // namespace kuzu
