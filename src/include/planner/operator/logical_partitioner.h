#pragma once

#include "binder/copy/bound_copy_from.h"
#include "common/column_data_format.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalPartitionerInfo {
    common::idx_t keyIdx;
    common::ColumnDataFormat dataFormat;

    LogicalPartitionerInfo(common::idx_t keyIdx, common::ColumnDataFormat dataFormat)
        : keyIdx{keyIdx}, dataFormat{dataFormat} {}
    EXPLICIT_COPY_DEFAULT_MOVE(LogicalPartitionerInfo);

private:
    LogicalPartitionerInfo(const LogicalPartitionerInfo& other)
        : keyIdx{other.keyIdx}, dataFormat{other.dataFormat} {}
};

class LogicalPartitioner : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::PARTITIONER;

public:
    LogicalPartitioner(std::vector<LogicalPartitionerInfo> infos,
        binder::BoundCopyFromInfo copyFromInfo, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType_, std::move(child)}, infos{std::move(infos)},
          copyFromInfo{std::move(copyFromInfo)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    common::idx_t getNumInfos() const { return infos.size(); }
    const LogicalPartitionerInfo* getInfo(common::idx_t idx) const {
        KU_ASSERT(idx < infos.size());
        return &infos[idx];
    }

    std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalPartitioner>(copyVector(infos), copyFromInfo.copy(),
            children[0]->copy());
    }

private:
    std::vector<LogicalPartitionerInfo> infos;

public:
    binder::BoundCopyFromInfo copyFromInfo;
};

} // namespace planner
} // namespace kuzu
