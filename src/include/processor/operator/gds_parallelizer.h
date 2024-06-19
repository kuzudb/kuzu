#pragma once

#include "function/table_functions.h"
#include "gds_call.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

using gds_algofunc_t =
    std::function<uint64_t(std::shared_ptr<GDSCallSharedState>&, GDSLocalState*)>;

class GDSParallelizerSharedState {
public:
    static constexpr const int64_t MORSEL_SIZE = 1000000;
    GDSParallelizerSharedState(int64_t count) : count{count} {
        nextMorsel.store(0);
        sum.store(0);
    };

public:
    int64_t count;
    std::atomic<int64_t> nextMorsel;
    std::atomic<int64_t> sum;
};

class GDSParallelizer : public Sink {
    static constexpr PhysicalOperatorType operatorType_ = PhysicalOperatorType::GDS_PARALLELIZER;

public:
    GDSParallelizer(std::shared_ptr<GDSParallelizerSharedState> sharedState, uint32_t id)
        : Sink{std::make_unique<ResultSetDescriptor>(), operatorType_, id,
              "" /* empty paramsString */}, sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    bool isParallel() const override { return true; }

    void initLocalStateInternal(ResultSet*, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<GDSParallelizer>(sharedState, id);
    }

private:
    std::shared_ptr<GDSParallelizerSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu