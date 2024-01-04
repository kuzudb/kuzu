#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalLoadExtension final : public LogicalOperator {
public:
    explicit LogicalLoadExtension(std::string path)
        : LogicalOperator{LogicalOperatorType::LOAD_EXTENSION}, path{std::move(path)} {}

    inline std::string getExpressionsForPrinting() const override { return path; }

    inline void computeFlatSchema() override { createEmptySchema(); }
    inline void computeFactorizedSchema() override { createEmptySchema(); }

    inline std::string getPath() { return path; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalLoadExtension>(path);
    }

private:
    std::string path;
};

} // namespace planner
} // namespace kuzu
