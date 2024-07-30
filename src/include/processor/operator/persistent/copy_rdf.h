#pragma once

#include "processor/operator/sink.h"
namespace kuzu {
namespace processor {

struct CopyRdfPrintInfo final : OPPrintInfo {
    std::string tableName;

    explicit CopyRdfPrintInfo(std::string tableName) : tableName(std::move(tableName)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<CopyRdfPrintInfo>(new CopyRdfPrintInfo(*this));
    }

private:
    CopyRdfPrintInfo(const CopyRdfPrintInfo& other)
        : OPPrintInfo(other), tableName(other.tableName) {}
};

struct CopyRdfSharedState {
    std::shared_ptr<FactorizedTable> fTable;
};

class CopyRdf : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::COPY_RDF;

public:
    CopyRdf(std::shared_ptr<CopyRdfSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(resultSetDescriptor), type_, id, std::move(printInfo)},
          sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    void executeInternal(ExecutionContext*) override {}

    void finalize(ExecutionContext*) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyRdf>(sharedState, resultSetDescriptor->copy(), id,
            printInfo->copy());
    }

private:
    std::shared_ptr<CopyRdfSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
