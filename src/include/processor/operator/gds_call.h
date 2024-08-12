#pragma once

#include "common/mask.h"
#include "function/gds/gds.h"
#include "function/gds/gds_utils.h"
#include "graph/graph.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

struct GDSCallSharedState {
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> fTable;
    std::unique_ptr<graph::Graph> graph;
    common::table_id_map_t<std::unique_ptr<common::NodeOffsetLevelSemiMask>> inputNodeOffsetMasks;

    GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable, std::unique_ptr<graph::Graph> graph,
        common::table_id_map_t<std::unique_ptr<common::NodeOffsetLevelSemiMask>>
            inputNodeOffsetMasks)
        : fTable{std::move(fTable)}, graph{std::move(graph)},
          inputNodeOffsetMasks{std::move(inputNodeOffsetMasks)} {}
    DELETE_COPY_AND_MOVE(GDSCallSharedState);
};

struct GDSCallInfo {
    std::unique_ptr<function::GDSAlgorithm> gds;

    explicit GDSCallInfo(std::unique_ptr<function::GDSAlgorithm> gds) : gds{std::move(gds)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(GDSCallInfo);

private:
    GDSCallInfo(const GDSCallInfo& other) : gds{other.gds->copy()} {}
};

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
    GDSCall(std::unique_ptr<ResultSetDescriptor> descriptor, GDSCallInfo info,
        std::shared_ptr<GDSCallSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(descriptor), operatorType_, id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    bool hasSemiMask() const { return !sharedState->inputNodeOffsetMasks.empty(); }
    std::vector<common::NodeSemiMask*> getSemiMasks() const;

    bool isSource() const override { return true; }

    bool isParallel() const override { return false; }

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    void executeInternal(ExecutionContext* executionContext) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<GDSCall>(resultSetDescriptor->copy(), info.copy(), sharedState, id,
            printInfo->copy());
    }

private:
    GDSCallInfo info;
    std::shared_ptr<GDSCallSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
