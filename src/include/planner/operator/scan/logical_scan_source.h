#pragma once

#include "binder/copy/bound_table_scan_info.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

// TODO(Xiyang): consider merging this operator with LogicalTableFunctionCall
class LogicalScanSource : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::SCAN_SOURCE;

public:
    LogicalScanSource(binder::BoundTableScanSourceInfo info,
        std::shared_ptr<binder::Expression> offset)
        : LogicalOperator{operatorType_}, info{std::move(info)}, offset{std::move(offset)} {}

    std::string getExpressionsForPrinting() const override { return std::string(); }

    const binder::BoundTableScanSourceInfo* getInfo() const { return &info; }
    bool hasOffset() const { return offset != nullptr; }
    std::shared_ptr<binder::Expression> getOffset() const { return offset; }

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalScanSource>(info.copy(), offset);
    }

private:
    binder::BoundTableScanSourceInfo info;
    // ScanFile may be used as a source operator for COPY pipeline. In such case, row offset needs
    // to be provided in order to generate internal ID.
    std::shared_ptr<binder::Expression> offset;
};

} // namespace planner
} // namespace kuzu
