#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class ImportDB final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::IMPORT_DATABASE;

public:
    ImportDB(std::string query, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, query{query} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ImportDB>(query, outputPos, id, printInfo->copy());
    }

private:
    std::string query;
};
} // namespace processor
} // namespace kuzu
