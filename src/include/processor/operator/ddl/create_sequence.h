#pragma once

#include "binder/ddl/bound_create_sequence_info.h"
#include "processor/operator/ddl/ddl.h"

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

class CreateSequence : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_SEQUENCE;

public:
    CreateSequence(binder::BoundCreateSequenceInfo info, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{type_, outputPos, id, std::move(printInfo)}, info{std::move(info)} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateSequence>(info.copy(), outputPos, id, printInfo->copy());
    }

private:
    binder::BoundCreateSequenceInfo info;
};

} // namespace processor
} // namespace kuzu
