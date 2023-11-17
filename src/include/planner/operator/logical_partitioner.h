#pragma once

#include "catalog/table_schema.h"
#include "common/column_data_format.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalPartitionerInfo {
    std::shared_ptr<binder::Expression> key;
    binder::expression_vector payloads;
    common::ColumnDataFormat dataFormat;
    catalog::TableSchema* tableSchema;

    LogicalPartitionerInfo(std::shared_ptr<binder::Expression> key,
        binder::expression_vector payloads, common::ColumnDataFormat dataFormat,
        catalog::TableSchema* tableSchema)
        : key{std::move(key)}, payloads{std::move(payloads)}, dataFormat{dataFormat},
          tableSchema{tableSchema} {}
    LogicalPartitionerInfo(const LogicalPartitionerInfo& other)
        : key{other.key}, payloads{other.payloads}, dataFormat{other.dataFormat},
          tableSchema{other.tableSchema} {}

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
        KU_ASSERT(idx < infos.size());
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
