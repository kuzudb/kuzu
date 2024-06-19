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
    LogicalPartitionerInfo(const LogicalPartitionerInfo& other)
        : keyIdx{other.keyIdx}, dataFormat{other.dataFormat} {}

    inline std::unique_ptr<LogicalPartitionerInfo> copy() {
        return std::make_unique<LogicalPartitionerInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalPartitionerInfo>> copy(
        const std::vector<std::unique_ptr<LogicalPartitionerInfo>>& infos);
};

class LogicalPartitioner : public LogicalOperator {
public:
    LogicalPartitioner(std::vector<std::unique_ptr<LogicalPartitionerInfo>> infos,
        binder::BoundCopyFromInfo copyFromInfo, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::PARTITIONER, std::move(child)},
          infos{std::move(infos)}, copyFromInfo{std::move(copyFromInfo)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    inline common::idx_t getNumInfos() const { return infos.size(); }
    inline LogicalPartitionerInfo* getInfo(common::idx_t idx) const {
        KU_ASSERT(idx < infos.size());
        return infos[idx].get();
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalPartitioner>(LogicalPartitionerInfo::copy(infos),
            copyFromInfo.copy(), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalPartitionerInfo>> infos;

public:
    binder::BoundCopyFromInfo copyFromInfo;
};

} // namespace planner
} // namespace kuzu
