#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

struct DropTablePrintInfo final : OPPrintInfo {
    std::string tableName;

    explicit DropTablePrintInfo(std::string tableName) : tableName{std::move(tableName)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<DropTablePrintInfo>(new DropTablePrintInfo(*this));
    }

private:
    DropTablePrintInfo(const DropTablePrintInfo& other)
        : OPPrintInfo{other}, tableName{other.tableName} {}
};

class DropTable : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DROP_TABLE;

public:
    DropTable(std::string tableName, common::table_id_t tableID, const DataPos& outputPos,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{type_, outputPos, id, std::move(printInfo)}, tableName{std::move(tableName)},
          tableID{tableID} {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(tableName, tableID, outputPos, id, printInfo->copy());
    }

protected:
    std::string tableName;
    common::table_id_t tableID;
};

} // namespace processor
} // namespace kuzu
