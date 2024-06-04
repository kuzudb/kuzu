#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateTable : public DDL {
public:
    CreateTable(binder::BoundCreateTableInfo info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_TABLE, outputPos, id, paramsString},
          info{std::move(info)} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateTable>(info.copy(), outputPos, id, paramsString);
    }

private:
    binder::BoundCreateTableInfo info;
};

} // namespace processor
} // namespace kuzu
