#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/column_data_format.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalPartitioningInfo {
    std::shared_ptr<binder::Expression> key;
    binder::expression_vector payloads;

    LogicalPartitioningInfo(std::shared_ptr<binder::Expression> key,
        binder::expression_vector payloads)
        : key{std::move(key)}, payloads{std::move(payloads)} {}
    LogicalPartitioningInfo(const LogicalPartitioningInfo& other)
        : key{other.key}, payloads{other.payloads} {}

    EXPLICIT_COPY_METHOD(LogicalPartitioningInfo);
};

struct LogicalPartitionerInfo {
    catalog::TableCatalogEntry* tableEntry;
    std::shared_ptr<binder::Expression> offset;
    std::vector<LogicalPartitioningInfo> partitioningInfos;

    LogicalPartitionerInfo(catalog::TableCatalogEntry* tableEntry,
        std::shared_ptr<binder::Expression> offset)
        : tableEntry{tableEntry}, offset{std::move(offset)} {}
    LogicalPartitionerInfo(const LogicalPartitionerInfo& other)
        : tableEntry{other.tableEntry}, offset{other.offset} {
        for (auto& partitioningInfo : other.partitioningInfos) {
            partitioningInfos.push_back(partitioningInfo.copy());
        }
    }

    EXPLICIT_COPY_METHOD(LogicalPartitionerInfo);

    common::idx_t getNumInfos() const { return partitioningInfos.size(); }
    LogicalPartitioningInfo& getInfo(common::idx_t idx) {
        KU_ASSERT(idx < partitioningInfos.size());
        return partitioningInfos[idx];
    }
    const LogicalPartitioningInfo& getInfo(common::idx_t idx) const {
        KU_ASSERT(idx < partitioningInfos.size());
        return partitioningInfos[idx];
    }
};

class LogicalPartitioner final : public LogicalOperator {
public:
    LogicalPartitioner(LogicalPartitionerInfo info, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::PARTITIONER, std::move(child)},
          info{std::move(info)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override;

    LogicalPartitionerInfo& getInfo() { return info; }
    const LogicalPartitionerInfo& getInfo() const { return info; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalPartitioner>(info.copy(), children[0]->copy());
    }

private:
    LogicalPartitionerInfo info;
};

} // namespace planner
} // namespace kuzu
