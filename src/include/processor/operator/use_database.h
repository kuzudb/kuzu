#pragma once

#include "processor/operator/database_operator.h"

namespace kuzu {
namespace processor {

class UseDatabase final : public DatabaseOperator {
public:
    UseDatabase(std::string dbName, uint32_t id, const std::string& paramsString)
        : DatabaseOperator{PhysicalOperatorType::USE_DATABASE, std::move(dbName), id,
              paramsString} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<UseDatabase>(dbName, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
