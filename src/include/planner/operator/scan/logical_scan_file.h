#pragma once

#include "binder/copy/bound_file_scan_info.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanFile : public LogicalOperator {
public:
    LogicalScanFile(binder::BoundFileScanInfo info, std::shared_ptr<binder::Expression> offset)
        : LogicalOperator{LogicalOperatorType::SCAN_FILE}, info{std::move(info)},
          offset{std::move(offset)} {}

    std::string getExpressionsForPrinting() const override { return std::string(); }

    const binder::BoundFileScanInfo* getInfo() const { return &info; }
    bool hasOffset() const { return offset != nullptr; }
    std::shared_ptr<binder::Expression> getOffset() const { return offset; }

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalScanFile>(info.copy(), offset);
    }

private:
    binder::BoundFileScanInfo info;
    // ScanFile may be used as a source operator for COPY pipeline. In such case, row offset needs
    // to be provided in order to generate internal ID.
    std::shared_ptr<binder::Expression> offset;
};

} // namespace planner
} // namespace kuzu
