#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class ImportDB final : public Simple {
public:
    ImportDB(std::string query, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : Simple{PhysicalOperatorType::IMPORT_DATABASE, outputPos, id, paramsString}, query{query} {
    }

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ImportDB>(query, outputPos, id, paramsString);
    }

private:
    std::string query;
};
} // namespace processor
} // namespace kuzu
