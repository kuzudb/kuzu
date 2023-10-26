#pragma once

#include "common/column_data_format.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalPartitionerInfo {
    std::shared_ptr<binder::Expression> key;
    std::vector<std::shared_ptr<binder::Expression>> payload;
    common::ColumnDataFormat dataFormat;

    LogicalPartitionerInfo(std::shared_ptr<binder::Expression> key,
        std::vector<std::shared_ptr<binder::Expression>> payload,
        common::ColumnDataFormat dataFormat)
        : key{std::move(key)}, payload{std::move(payload)}, dataFormat{dataFormat} {}
    LogicalPartitionerInfo(const LogicalPartitionerInfo& other)
        : key{other.key}, payload{other.payload}, dataFormat{other.dataFormat} {}

    inline std::unique_ptr<LogicalPartitionerInfo> copy() {
        return std::make_unique<LogicalPartitionerInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalPartitionerInfo>> copy(
        const std::vector<std::unique_ptr<LogicalPartitionerInfo>>& infos);
};

class LogicalPartitioner : public LogicalOperator {
public:
    LogicalPartitioner(std::vector<std::unique_ptr<LogicalPartitionerInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::PARTITIONER, std::move(child)}, infos{std::move(
                                                                                   infos)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    inline common::vector_idx_t getNumInfos() { return infos.size(); }
    inline LogicalPartitionerInfo* getInfo(common::vector_idx_t idx) {
        assert(idx < infos.size());
        return infos[idx].get();
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalPartitioner>(
            LogicalPartitionerInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalPartitionerInfo>> infos;
};

} // namespace planner
} // namespace kuzu
