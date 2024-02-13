#pragma once
#include "binder/binder.h"
#include "binder/bound_statement.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"

namespace kuzu {
namespace binder {

class BoundExportDatabase : public BoundStatement {
public:
    BoundExportDatabase(std::string filePath, common::FileType fileType,
        std::vector<ExportedTableData> exportData, common::CSVOption csvOption)
        : BoundStatement{common::StatementType::EXPORT_DATABASE,
              BoundStatementResult::createEmptyResult()},
          filePath{std::move(filePath)}, fileType{fileType},
          exportData(std::move(exportData)), csvOption{std::move(csvOption)} {}

    inline std::string getFilePath() const { return filePath; }
    inline common::FileType getFileType() const { return fileType; }
    inline const common::CSVOption* getCopyOption() const { return &csvOption; }
    inline const std::vector<ExportedTableData>* getExportData() const { return &exportData; }

private:
    std::string filePath;
    common::FileType fileType;
    std::vector<ExportedTableData> exportData;
    common::CSVOption csvOption;
};

} // namespace binder
} // namespace kuzu
