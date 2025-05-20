#pragma once

#include "common/exception/internal.h"
#include "common/metric.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set_descriptor.h"

namespace kuzu {
namespace processor {

class KUZU_API Sink : public PhysicalOperator {
public:
    Sink(PhysicalOperatorType operatorType, physical_op_id id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{operatorType, id, std::move(printInfo)} {}
    Sink(PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> child,
        physical_op_id id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{operatorType, std::move(child), id, std::move(printInfo)} {}

    bool isSink() const override { return true; }

    void setDescriptor(std::unique_ptr<ResultSetDescriptor> descriptor) {
        KU_ASSERT(resultSetDescriptor == nullptr);
        resultSetDescriptor = std::move(descriptor);
    }
    std::unique_ptr<ResultSet> getResultSet(storage::MemoryManager* memoryManager);

    void execute(ResultSet* resultSet, ExecutionContext* context) {
        initLocalState(resultSet, context);
        metrics->executionTime.start();
        executeInternal(context);
        metrics->executionTime.stop();
    }

    std::unique_ptr<PhysicalOperator> copy() override = 0;

protected:
    virtual void executeInternal(ExecutionContext* context) = 0;

    bool getNextTuplesInternal(ExecutionContext* /*context*/) final {
        throw common::InternalException(
            "getNextTupleInternal() should not be called on sink operator.");
    }

protected:
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor;
};

class KUZU_API DummySink final : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DUMMY_SINK;

public:
    DummySink(std::unique_ptr<PhysicalOperator> child, uint32_t id)
        : Sink{type_, std::move(child), id, OPPrintInfo::EmptyInfo()} {}

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<DummySink>(children[0]->copy(), id);
    }

protected:
    void executeInternal(ExecutionContext* context) override {
        while (children[0]->getNextTuple(context)) {
            // DO NOTHING.
        }
    }
};

} // namespace processor
} // namespace kuzu
