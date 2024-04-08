#pragma once

#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

struct CopyRdfSharedState {
    std::shared_ptr<FactorizedTable> fTable;
};

class CopyRdf : public Sink {
public:
    CopyRdf(std::shared_ptr<CopyRdfSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_RDF, id, paramsString},
          sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    void executeInternal(ExecutionContext*) override {}

    void finalize(ExecutionContext*) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyRdf>(sharedState, resultSetDescriptor->copy(), id,
            paramsString);
    }

private:
    std::shared_ptr<CopyRdfSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
