#pragma once

#include "simple.h"

namespace kuzu {
namespace processor {

class DatabaseOperator : public Simple {
public:
    DatabaseOperator(PhysicalOperatorType physicalOperator, std::string dbName,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : Simple{physicalOperator, outputPos, id, paramsString}, dbName{std::move(dbName)} {}

protected:
    std::string dbName;
};

} // namespace processor
} // namespace kuzu
