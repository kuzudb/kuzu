#pragma once
#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundImportDatabase final : public BoundStatement {
public:
    BoundImportDatabase(std::string filePath, std::string query)
        : BoundStatement{common::StatementType::IMPORT_DATABASE,
              BoundStatementResult::createEmptyResult()},
          filePath{std::move(filePath)}, query{query} {}

    inline std::string getFilePath() const { return filePath; }
    inline std::string getQuery() const { return query; }

private:
    std::string filePath;
    std::string query;
};

} // namespace binder
} // namespace kuzu
