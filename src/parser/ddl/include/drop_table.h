#pragma once

#include "src/parser/ddl/include/ddl.h"

namespace kuzu {
namespace parser {

using namespace std;

class DropTable : public DDL {
public:
    explicit DropTable(string tableName) : DDL{StatementType::DROP_TABLE, move(tableName)} {}
};

} // namespace parser
} // namespace kuzu
