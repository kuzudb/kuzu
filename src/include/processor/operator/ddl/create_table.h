#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class CreateTable final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_TABLE;

public:
    CreateTable(binder::BoundCreateTableInfo info, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, info{std::move(info)},
          tableCreated{false} {}

    void executeInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<CreateTable>(info.copy(), outputPos, id, printInfo->copy());
    }

private:
    binder::BoundCreateTableInfo info;
    bool tableCreated;
};

} // namespace processor
} // namespace kuzu
