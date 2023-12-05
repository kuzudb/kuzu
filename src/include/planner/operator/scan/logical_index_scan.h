#pragma once

#include "binder/copy/index_look_up_info.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalIndexScanNode : public LogicalOperator {
public:
    LogicalIndexScanNode(std::vector<std::unique_ptr<binder::IndexLookupInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::INDEX_SCAN_NODE, std::move(child)}, infos{std::move(
                                                                                       infos)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override;

    inline uint32_t getNumInfos() const { return infos.size(); }
    inline binder::IndexLookupInfo* getInfo(uint32_t idx) const { return infos[idx].get(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalIndexScanNode>(
            binder::IndexLookupInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<binder::IndexLookupInfo>> infos;
};

} // namespace planner
} // namespace kuzu
