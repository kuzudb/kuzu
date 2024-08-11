#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "processor/operator/ddl/ddl.h"
#include "planner/operator/ddl/base_create_table.h"

namespace kuzu {
namespace processor {

struct CreateTablePrintInfo final : OPPrintInfo {
    std::unique_ptr<BaseCreateTablePrintInfo> base;

    CreateTablePrintInfo(std::unique_ptr<BaseCreateTablePrintInfo> base)
        : base(std::move(base)) {}

    std::string toString() const override {return base->toString();};

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<CreateTablePrintInfo>(new CreateTablePrintInfo(*this));
    }

private:
    CreateTablePrintInfo(const CreateTablePrintInfo& other)
        : OPPrintInfo{other}, base(std::make_unique<BaseCreateTablePrintInfo>(*other.base)) {}
};

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

} // namespace processor
} // namespace kuzu
