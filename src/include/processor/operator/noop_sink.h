#pragma once

#include "function/gds/gds.h"
#include "function/gds/gds_utils.h"
#include "graph/graph.h"
#include "processor/operator/mask.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

class NOOPSink : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::NOOP_SINK;
public:
    NOOPSink(std::unique_ptr<ResultSetDescriptor> descriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink(std::move(descriptor), type_, std::move(child), id, std::move(printInfo)) {}

    void initLocalStateInternal(ResultSet* /*resultSet*/, ExecutionContext* /*executionContext*/) override {}

    void executeInternal(ExecutionContext* context) override {
        while (children[0]->getNextTuple(context)) {
            // Do nothing
        }
    }

    void finalize(ExecutionContext* /*context*/) override {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<NOOPSink>(resultSetDescriptor->copy(), children[0]->clone(), id,
            printInfo->copy());
    }
};

} // namespace processor
} // namespace kuzu