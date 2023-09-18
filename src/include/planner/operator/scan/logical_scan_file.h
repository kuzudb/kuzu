#pragma once

#include "binder/copy/bound_file_scan_info.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanFile : public LogicalOperator {
public:
    LogicalScanFile(std::unique_ptr<binder::BoundFileScanInfo> info)
        : LogicalOperator{LogicalOperatorType::SCAN_FILE}, info{std::move(info)} {}

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline binder::BoundFileScanInfo* getInfo() const { return info.get(); }

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalScanFile>(info->copy());
    }

private:
    std::unique_ptr<binder::BoundFileScanInfo> info;
};

} // namespace planner
} // namespace kuzu
