#pragma once

#include "binder/ddl/bound_create_sequence_info.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

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

struct CreateSequencePrintInfo final : OPPrintInfo {
    std::string sequenceName;

    CreateSequencePrintInfo(std::string sequenceName)
        : sequenceName{std::move(sequenceName)} {}
    CreateSequencePrintInfo(const CreateSequencePrintInfo& other)
        : OPPrintInfo{other}, sequenceName{other.sequenceName} {}
    
    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<CreateSequencePrintInfo>(*this);
    }};

} // namespace processor
} // namespace kuzu
