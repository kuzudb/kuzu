#pragma once

#include "binder/bound_statement.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"

namespace kuzu {
namespace binder {

class BoundCopyTo : public BoundStatement {
public:
    BoundCopyTo(std::string filePath, common::FileType fileType,
        std::unique_ptr<BoundStatement> query, common::CSVOption csvOption)
        : BoundStatement{common::StatementType::COPY_TO, BoundStatementResult::createEmptyResult()},
          filePath{std::move(filePath)}, fileType{fileType}, query{std::move(query)},
          csvOption{std::move(csvOption)} {}

    inline std::string getFilePath() const { return filePath; }
    inline common::FileType getFileType() const { return fileType; }

    inline const BoundStatement* getRegularQuery() const { return query.get(); }
    inline const common::CSVOption* getCopyOption() const { return &csvOption; }

private:
    std::string filePath;
    common::FileType fileType;
    std::unique_ptr<BoundStatement> query;
    common::CSVOption csvOption;
};

} // namespace binder
} // namespace kuzu
