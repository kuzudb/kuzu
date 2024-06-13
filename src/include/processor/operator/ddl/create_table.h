#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateTable : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_TABLE;

public:
    CreateTable(binder::BoundCreateTableInfo info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{type_, outputPos, id, paramsString}, info{std::move(info)}, tableCreated{false} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateTable>(info.copy(), outputPos, id, paramsString);
    }

private:
    binder::BoundCreateTableInfo info;
    bool tableCreated = false;
};

} // namespace processor
} // namespace kuzu
