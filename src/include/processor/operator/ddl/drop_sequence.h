#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class DropSequence : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DROP_SEQUENCE;

public:
    DropSequence(std::string sequenceName, common::sequence_id_t sequenceID,
        const DataPos& outputPos, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{type_, outputPos, id, std::move(printInfo)}, sequenceName{std::move(sequenceName)},
          sequenceID{sequenceID} {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropSequence>(sequenceName, sequenceID, outputPos, id,
            printInfo->copy());
    }

protected:
    std::string sequenceName;
    common::sequence_id_t sequenceID;
};

} // namespace processor
} // namespace kuzu
