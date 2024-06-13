
#pragma once

#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateType : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_TYPE;

public:
    CreateType(std::string name, common::LogicalType type, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{type_, outputPos, id, std::move(printInfo)}, name{std::move(name)},
          type{std::move(type)} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateType>(name, type.copy(), outputPos, id, printInfo->copy());
    }

private:
    std::string name;
    common::LogicalType type;
};

} // namespace processor
} // namespace kuzu
