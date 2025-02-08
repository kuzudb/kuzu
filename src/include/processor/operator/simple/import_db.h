#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class ImportDB final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::IMPORT_DATABASE;

public:
    ImportDB(std::string query, std::string indexQuery, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, query{std::move(query)},
          indexQuery{std::move(indexQuery)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<ImportDB>(query, indexQuery, outputPos, id, printInfo->copy());
    }

private:
    std::string query;
    std::string indexQuery;
};

} // namespace processor
} // namespace kuzu
