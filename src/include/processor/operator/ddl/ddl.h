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
    DDL(PhysicalOperatorType operatorType, Catalog* catalog, const DataPos& outputPos, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, catalog{catalog}, outputPos{outputPos} {
    }
    virtual ~DDL() override = default;

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

protected:
    virtual std::string getOutputMsg() = 0;
    virtual void executeDDLInternal() = 0;

protected:
    Catalog* catalog;
    DataPos outputPos;
    ValueVector* outputVector;

    bool hasExecuted = false;
};

} // namespace processor
} // namespace kuzu
