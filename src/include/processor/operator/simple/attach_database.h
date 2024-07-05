#pragma once

#include "binder/bound_attach_info.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

struct AttachDatabasePrintInfo final : OPPrintInfo {
    std::string dbName;
    std::string dbPath;

    AttachDatabasePrintInfo(std::string dbName, std::string dbPath)
        : dbName{std::move(dbName)}, dbPath{std::move(dbPath)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<AttachDatabasePrintInfo>(new AttachDatabasePrintInfo(*this));
    }

private:
    AttachDatabasePrintInfo(const AttachDatabasePrintInfo& other)
        : OPPrintInfo{other}, dbName{other.dbName}, dbPath{other.dbPath} {}
};

class AttachDatabase final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::ATTACH_DATABASE;

public:
    AttachDatabase(binder::AttachInfo attachInfo, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, attachInfo{std::move(attachInfo)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<AttachDatabase>(attachInfo, outputPos, id, printInfo->copy());
    }

private:
    binder::AttachInfo attachInfo;
};

} // namespace processor
} // namespace kuzu
