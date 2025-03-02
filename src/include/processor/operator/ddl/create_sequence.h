#pragma once

#include "binder/ddl/bound_create_sequence_info.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

struct CreateSequencePrintInfo final : OPPrintInfo {
    std::string seqName;

    explicit CreateSequencePrintInfo(std::string seqName) : seqName{std::move(seqName)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<CreateSequencePrintInfo>(new CreateSequencePrintInfo(*this));
    }

private:
    CreateSequencePrintInfo(const CreateSequencePrintInfo& other)
        : OPPrintInfo{other}, seqName{other.seqName} {}
};

class CreateSequence final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_SEQUENCE;

public:
    CreateSequence(binder::BoundCreateSequenceInfo info, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, info{std::move(info)} {}

    void executeInternal(ExecutionContext* context) final;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<CreateSequence>(info.copy(), outputPos, id, printInfo->copy());
    }

private:
    binder::BoundCreateSequenceInfo info;
};

} // namespace processor
} // namespace kuzu
