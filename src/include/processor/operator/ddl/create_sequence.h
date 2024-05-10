#pragma once

#include "binder/ddl/bound_create_sequence_info.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateSequence : public DDL {
public:
    CreateSequence(binder::BoundCreateSequenceInfo info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_SEQUENCE, outputPos, id, paramsString},
          info{std::move(info)} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateSequence>(info.copy(), outputPos, id, paramsString);
    }

private:
    binder::BoundCreateSequenceInfo info;
};

} // namespace processor
} // namespace kuzu
