#pragma once

#include "common/exception/internal.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set_descriptor.h"

namespace kuzu {
namespace processor {

class Sink : public PhysicalOperator {
public:
    Sink(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        PhysicalOperatorType operatorType, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{operatorType, id, std::move(printInfo)},
          resultSetDescriptor{std::move(resultSetDescriptor)} {}
    Sink(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{operatorType, std::move(child), id, std::move(printInfo)},
          resultSetDescriptor{std::move(resultSetDescriptor)} {}

    bool isSink() const override { return true; }

    ResultSetDescriptor* getResultSetDescriptor() { return resultSetDescriptor.get(); }

    void execute(ResultSet* resultSet, ExecutionContext* context) {
        initLocalState(resultSet, context);
        metrics->executionTime.start();
        executeInternal(context);
        metrics->executionTime.stop();
    }

    virtual void finalize(ExecutionContext* /*context*/) {};

    std::unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    virtual void executeInternal(ExecutionContext* context) = 0;

    bool getNextTuplesInternal(ExecutionContext* /*context*/) final {
        throw common::InternalException(
            "getNextTupleInternal() should not be called on sink operator.");
    }

protected:
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor;
};

} // namespace processor
} // namespace kuzu
