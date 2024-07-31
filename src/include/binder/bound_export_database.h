#pragma once
#include "binder/binder.h"
#include "binder/bound_statement.h"
#include "binder/query/bound_regular_query.h"
#include "common/copier_config/reader_config.h"

namespace kuzu {
namespace binder {

struct ExportedTableData {
    std::string tableName;
    std::unique_ptr<BoundRegularQuery> regularQuery;
    std::vector<std::string> columnNames;
    std::vector<common::LogicalType> columnTypes;

    const std::vector<common::LogicalType>& getColumnTypesRef() const { return columnTypes; }
    const BoundRegularQuery* getRegularQuery() const { return regularQuery.get(); }
};

class BoundExportDatabase final : public BoundStatement {
public:
    BoundExportDatabase(std::string filePath, common::FileTypeInfo fileTypeInfo,
        std::vector<ExportedTableData> exportData,
        std::unordered_map<std::string, common::Value> csvOption)
        : BoundStatement{common::StatementType::EXPORT_DATABASE,
              BoundStatementResult::createSingleStringColumnResult()},
          exportData(std::move(exportData)),
          boundFileInfo(std::move(fileTypeInfo), std::vector{std::move(filePath)}) {
        boundFileInfo.options = std::move(csvOption);
    }

    std::string getFilePath() const { return boundFileInfo.filePaths[0]; }
    common::FileType getFileType() const { return boundFileInfo.fileTypeInfo.fileType; }
    std::unordered_map<std::string, common::Value> getExportOptions() const {
        return boundFileInfo.options;
    }
    const common::ReaderConfig* getBoundFileInfo() const { return &boundFileInfo; }
    const std::vector<ExportedTableData>* getExportData() const { return &exportData; }

private:
    std::vector<ExportedTableData> exportData;
    common::ReaderConfig boundFileInfo;
};

} // namespace binder
} // namespace kuzu
