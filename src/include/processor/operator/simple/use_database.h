#pragma once

#include "database_operator.h"

namespace kuzu {
namespace processor {

class UseDatabase final : public DatabaseOperator {
public:
    UseDatabase(std::string dbName, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DatabaseOperator{PhysicalOperatorType::USE_DATABASE, std::move(dbName), outputPos, id,
              paramsString} {}

    void executeInternal(ExecutionContext* context) final;
    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<UseDatabase>(dbName, outputPos, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
