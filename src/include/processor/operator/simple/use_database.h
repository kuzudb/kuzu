#pragma once

#include "simple.h"

namespace kuzu {
namespace processor {

struct UseDatabasePrintInfo final : OPPrintInfo {
    std::string dbName;

    explicit UseDatabasePrintInfo(std::string dbName) : dbName(std::move(dbName)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<UseDatabasePrintInfo>(new UseDatabasePrintInfo(*this));
    }

private:
    UseDatabasePrintInfo(const UseDatabasePrintInfo& other)
        : OPPrintInfo(other), dbName(other.dbName) {}
};

class UseDatabase final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::USE_DATABASE;

public:
    UseDatabase(std::string dbName, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, dbName{std::move(dbName)} {}

    void executeInternal(ExecutionContext* context) final;
    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<UseDatabase>(dbName, outputPos, id, printInfo->copy());
    }

private:
    std::string dbName;
};

} // namespace processor
} // namespace kuzu
