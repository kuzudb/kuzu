#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateTable : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_TABLE;

public:
    CreateTable(binder::BoundCreateTableInfo info, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{type_, outputPos, id, std::move(printInfo)}, info{std::move(info)},
          tableCreated{false} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateTable>(info.copy(), outputPos, id, printInfo->copy());
    }

private:
    binder::BoundCreateTableInfo info;
    bool tableCreated = false;
};

struct CreateTablePrintInfo final : OPPrintInfo {
    std::string tableName;
    // std::string tableConfig;

    CreateTablePrintInfo(std::string tableName)
        : tableName{std::move(tableName)} {}
    CreateTablePrintInfo(const CreateTablePrintInfo& other)
        : OPPrintInfo{other}, tableName{other.tableName} {}
    
    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<CreateTablePrintInfo>(*this);
    }
};


} // namespace processor
} // namespace kuzu
