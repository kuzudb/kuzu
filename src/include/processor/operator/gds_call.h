#pragma once

#include "function/gds/gds.h"
#include "graph/graph.h"
#include "graph/graph_entry.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

struct GDSCallSharedState {
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> fTable;
    std::unique_ptr<graph::Graph> graph;

    explicit GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable)
        : fTable{std::move(fTable)} {}
};

struct GDSCallInfo {
    std::unique_ptr<function::GDSAlgorithm> gds;
    graph::GraphEntry graphEntry;

    GDSCallInfo(std::unique_ptr<function::GDSAlgorithm> gds, graph::GraphEntry graphEntry)
        : gds{std::move(gds)}, graphEntry{std::move(graphEntry)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(GDSCallInfo);

private:
    GDSCallInfo(const GDSCallInfo& other)
        : gds{other.gds->copy()}, graphEntry{other.graphEntry.copy()} {}
};

class GDSCall : public Sink {
    static constexpr PhysicalOperatorType operatorType_ = PhysicalOperatorType::GDS_CALL;

public:
    GDSCall(std::unique_ptr<ResultSetDescriptor> descriptor, GDSCallInfo info,
        std::shared_ptr<GDSCallSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : Sink{std::move(descriptor), operatorType_, id, paramsString}, info{std::move(info)},
          sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    bool isParallel() const override { return false; }

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    void initGlobalStateInternal(ExecutionContext*) override;

    void executeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<GDSCall>(resultSetDescriptor->copy(), info.copy(), sharedState, id,
            paramsString);
    }

private:
    GDSCallInfo info;
    std::shared_ptr<GDSCallSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
