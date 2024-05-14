#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class DropSequence : public DDL {
public:
    DropSequence(std::string sequenceName, common::sequence_id_t sequenceID,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::DROP_TABLE, outputPos, id, paramsString},
          sequenceName{std::move(sequenceName)}, sequenceID{sequenceID} {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropSequence>(sequenceName, sequenceID, outputPos, id, paramsString);
    }

protected:
    std::string sequenceName;
    common::sequence_id_t sequenceID;
};

} // namespace processor
} // namespace kuzu
