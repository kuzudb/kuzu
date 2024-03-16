#pragma once

#include <string>

#include "function/table_functions.h"

namespace kuzu {
namespace main {

class AttachedDatabase {
public:
    AttachedDatabase(std::string dbName, function::TableFunction scanFunction)
        : dbName{std::move(dbName)}, scanFunction{std::move(scanFunction)} {}

    std::string getDBName() { return dbName; }

    function::TableFunction getScanFunction() { return scanFunction; }

private:
    std::string dbName;
    function::TableFunction scanFunction;
};

} // namespace main
} // namespace kuzu
