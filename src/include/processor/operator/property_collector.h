#pragma once

#include "common/types/internal_id_util.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct PropertyCollectorInfo {
    DataPos nodeIDPos;
    DataPos propPos;

    PropertyCollectorInfo(const DataPos& nodeIDPos, const DataPos& propPos)
        : nodeIDPos{nodeIDPos}, propPos{propPos} {}
    PropertyCollectorInfo(const PropertyCollectorInfo& other)
        : nodeIDPos{other.nodeIDPos}, propPos{other.propPos} {}

    std::unique_ptr<PropertyCollectorInfo> copy() const {
        return std::make_unique<PropertyCollectorInfo>(*this);
    }
};

struct PropertyCollectorPrintInfo final : OPPrintInfo {
    std::string operatorName;

    explicit PropertyCollectorPrintInfo(std::string operatorName)
        : operatorName{std::move(operatorName)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<PropertyCollectorPrintInfo>(new PropertyCollectorPrintInfo(*this));
    }

private:
    PropertyCollectorPrintInfo(const PropertyCollectorPrintInfo& other)
        : OPPrintInfo{other}, operatorName{other.operatorName} {}
};

struct PropertyCollectorSharedState {
    std::mutex mtx;
    // We should replace with a concurrent map.
    common::internal_id_map_t<uint64_t> nodeProperty;
};

struct PropertyCollectorLocalState {
    const common::ValueVector* nodeIDVector;
    const common::ValueVector* propertyVector;

    PropertyCollectorLocalState() = default;
    DELETE_COPY_DEFAULT_MOVE(PropertyCollectorLocalState);
};

class PropertyCollector : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PROPERTY_COLLECTOR;

public:
    PropertyCollector(PropertyCollectorInfo info,
        std::shared_ptr<PropertyCollectorSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, localState{}, sharedState{std::move(sharedState)} {}

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<PropertyCollector>(info, sharedState, children[0]->clone(), id,
            printInfo->copy());
    }

protected:
    PropertyCollectorInfo info;
    PropertyCollectorLocalState localState;
    std::shared_ptr<PropertyCollectorSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
