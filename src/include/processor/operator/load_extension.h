#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class LoadExtension final : public PhysicalOperator {
public:
    LoadExtension(std::string path, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::LOAD_EXTENSION, id, paramsString},
          path{std::move(path)}, hasExecuted{false} {}

    bool isSource() const override { return true; }
    bool canParallel() const override { return false; }

    void initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) override {
        hasExecuted = false;
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<LoadExtension>(path, id, paramsString);
    }

private:
    std::string path;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
