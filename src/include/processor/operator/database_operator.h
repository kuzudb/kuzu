#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class DatabaseOperator : public PhysicalOperator {
public:
    DatabaseOperator(PhysicalOperatorType physicalOperator, std::string dbName, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{physicalOperator, id, paramsString}, dbName{std::move(dbName)} {}

    bool isSource() const override { return true; }
    bool canParallel() const override { return false; }

protected:
    std::string dbName;
};

} // namespace processor
} // namespace kuzu
