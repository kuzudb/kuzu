#pragma once

#include "binder/query/bound_regular_query.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"

namespace kuzu {
namespace binder {

class BoundExportDatabase : public BoundStatement {
public:
    BoundExportDatabase(
        std::string filePath, common::FileType fileType, common::CSVOption csvOption)
        : BoundStatement{common::StatementType::EXPORT_DATABASE,
              BoundStatementResult::createEmptyResult()},
          filePath{std::move(filePath)}, fileType{fileType}, csvOption{std::move(csvOption)} {}

    inline std::string getFilePath() const { return filePath; }
    inline common::FileType getFileType() const { return fileType; }
    inline const common::CSVOption* getCopyOption() const { return &csvOption; }

private:
    std::string filePath;
    common::FileType fileType;
    common::CSVOption csvOption;
};

} // namespace binder
} // namespace kuzu
