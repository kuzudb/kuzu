#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalIndexScanNodeInfo {
    common::table_id_t nodeTableID;
    std::shared_ptr<binder::Expression> offset; // output
    std::shared_ptr<binder::Expression> key;    // input
    // TODO: the following field should be removed.
    std::unique_ptr<common::LogicalType> indexType;

    LogicalIndexScanNodeInfo(common::table_id_t nodeTableID,
        std::shared_ptr<binder::Expression> offset, std::shared_ptr<binder::Expression> key,
        std::unique_ptr<common::LogicalType> indexType)
        : nodeTableID{nodeTableID}, offset{std::move(offset)}, key{std::move(key)},
          indexType{std::move(indexType)} {}
    LogicalIndexScanNodeInfo(const LogicalIndexScanNodeInfo& other)
        : nodeTableID{other.nodeTableID}, offset{other.offset}, key{other.key},
          indexType{other.indexType->copy()} {}

    inline std::unique_ptr<LogicalIndexScanNodeInfo> copy() const {
        return std::make_unique<LogicalIndexScanNodeInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> copy(
        const std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>>& infos);
};

class LogicalIndexScanNode : public LogicalOperator {
public:
    LogicalIndexScanNode(std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::INDEX_SCAN_NODE, std::move(child)}, infos{std::move(
                                                                                       infos)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override;

    inline uint32_t getNumInfos() const { return infos.size(); }
    inline LogicalIndexScanNodeInfo* getInfo(uint32_t idx) const { return infos[idx].get(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalIndexScanNode>(
            LogicalIndexScanNodeInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> infos;
};

} // namespace planner
} // namespace kuzu
