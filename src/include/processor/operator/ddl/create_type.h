#pragma once

#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateType : public DDL {
public:
    CreateType(std::string name, common::LogicalType type, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_TYPE, outputPos, id, paramsString},
          name{std::move(name)}, type{std::move(type)} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateType>(name, type, outputPos, id, paramsString);
    }

private:
    std::string name;
    common::LogicalType type;
};

} // namespace processor
} // namespace kuzu
