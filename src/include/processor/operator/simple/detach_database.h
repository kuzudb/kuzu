#pragma once

#include "database_operator.h"

namespace kuzu {
namespace processor {

class DetachDatabase final : public DatabaseOperator {
public:
    DetachDatabase(std::string dbName, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DatabaseOperator{PhysicalOperatorType::DETACH_DATABASE, std::move(dbName), outputPos, id,
              paramsString} {}

    void executeInternal(ExecutionContext* context) final;
    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<DetachDatabase>(dbName, outputPos, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
