#pragma once

#include "catalog/catalog.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class DDL : public PhysicalOperator {
public:
    DDL(PhysicalOperatorType operatorType, catalog::Catalog* catalog, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, catalog{catalog}, outputPos{outputPos},
          outputVector{nullptr}, hasExecuted{false} {}

    inline bool isSource() const final { return true; }
    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    virtual std::string getOutputMsg() = 0;
    virtual void executeDDLInternal() = 0;

protected:
    catalog::Catalog* catalog;
    DataPos outputPos;
    common::ValueVector* outputVector;

    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
