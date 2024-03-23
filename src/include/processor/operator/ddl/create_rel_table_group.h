#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateRelTableGroup : public DDL {
public:
    CreateRelTableGroup(binder::BoundCreateTableInfo info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_REL_TABLE, outputPos, id, paramsString}, info{std::move(
                                                                                        info)} {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTableGroup>(info.copy(), outputPos, id, paramsString);
    }

private:
    binder::BoundCreateTableInfo info;
};

} // namespace processor
} // namespace kuzu
