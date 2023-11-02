#pragma once

#include "common/copier_config/copier_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyTo : public LogicalOperator {
public:
    LogicalCopyTo(
        std::shared_ptr<LogicalOperator> child, std::unique_ptr<common::ReaderConfig> config)
        : LogicalOperator{LogicalOperatorType::COPY_TO, std::move(child)}, config{
                                                                               std::move(config)} {}

    f_group_pos_set getGroupsPosToFlatten();

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    inline common::ReaderConfig* getConfig() const { return config.get(); }

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyTo>(children[0]->copy(), config->copy());
    }

private:
    std::shared_ptr<binder::Expression> outputExpression;
    std::unique_ptr<common::ReaderConfig> config;
};

} // namespace planner
} // namespace kuzu
