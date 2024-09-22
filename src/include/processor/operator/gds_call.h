#pragma once

#include "function/gds/gds.h"
#include "function/gds/gds_utils.h"
#include "gds_call_shared_state.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

struct GDSCallPrintInfo final : OPPrintInfo {
    std::string funcName;

    explicit GDSCallPrintInfo(std::string funcName) : funcName{std::move(funcName)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<GDSCallPrintInfo>(new GDSCallPrintInfo(*this));
    }

private:
    GDSCallPrintInfo(const GDSCallPrintInfo& other)
        : OPPrintInfo{other}, funcName{other.funcName} {}
};

class GDSCall : public Sink {
    static constexpr PhysicalOperatorType operatorType_ = PhysicalOperatorType::GDS_CALL;

public:
    GDSCall(std::unique_ptr<ResultSetDescriptor> descriptor,
        std::unique_ptr<function::GDSAlgorithm> _gds,
        std::shared_ptr<GDSCallSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(descriptor), operatorType_, id, std::move(printInfo)},
          gds{std::move(_gds)}, sharedState{std::move(sharedState)} {}

    bool hasSemiMask() const { return !sharedState->inputNodeOffsetMasks.empty(); }
    std::vector<common::NodeSemiMask*> getSemiMasks() const;

    bool isSource() const override { return true; }

    bool isParallel() const override { return false; }

    void initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) override {
        gds->setSharedState(sharedState);
    }

    void executeInternal(ExecutionContext* executionContext) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<GDSCall>(resultSetDescriptor->copy(), gds->copy(), sharedState, id,
            printInfo->copy());
    }

private:
    std::unique_ptr<function::GDSAlgorithm> gds;
    std::shared_ptr<GDSCallSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
