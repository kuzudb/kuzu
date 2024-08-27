#include "binder/binder.h"
#include "binder/bound_import_database.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/exception/binder.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "parser/copy.h"
#include "parser/parser.h"
#include "parser/port_db.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

static std::string getQueryFromFile(common::VirtualFileSystem* vfs, const std::string boundFilePath,
    const std::string fileName, main::ClientContext* context) {
    auto filePath = vfs->joinPath(boundFilePath, fileName);
    if (!vfs->fileOrPathExists(filePath, context)) {
        throw BinderException(stringFormat("File {} does not exist.", filePath));
    }
    auto fileInfo = vfs->openFile(filePath, FileFlags::READ_ONLY
#ifdef _WIN32
                                                | FileFlags::BINARY
#endif
    );
    auto fsize = fileInfo->getFileSize();
    auto buffer = std::make_unique<char[]>(fsize);
    fileInfo->readFile(buffer.get(), fsize);
    return std::string(buffer.get(), fsize);
}

static std::string getColumnNamesToCopy(const CopyFrom& copyFrom) {
    std::string columns = "";
    std::string delimiter = "";
    for (auto& column : copyFrom.getColumnNames()) {
        columns += delimiter;
        columns += column;
        if (delimiter == "") {
            delimiter = ",";
        }
    }
    if (columns.empty()) {
        return columns;
    }
    return common::stringFormat("({})", columns);
}

static std::string getCopyFilePath(const std::string& boundFilePath, const std::string& filePath) {
    if (filePath[0] == '/' || (std::isalpha(filePath[0]) && filePath[1] == ':')) {
        // Note:
        // Unix absolute path starts with '/'
        // Windows absolute path starts with "[DiskID]://"
        // This code path is for backward compatability, we used to export the absolute path for
        // csv files to copy.cypher files.
        return filePath;
    }
    return boundFilePath + "/" + filePath;
}

std::unique_ptr<BoundStatement> Binder::bindImportDatabaseClause(const Statement& statement) {
    auto& importDB = statement.constCast<ImportDB>();
    auto fs = clientContext->getVFSUnsafe();
    auto boundFilePath = fs->expandPath(clientContext, importDB.getFilePath());
    if (!fs->fileOrPathExists(boundFilePath, clientContext)) {
        throw BinderException(stringFormat("Directory {} does not exist.", boundFilePath));
    }
    std::string finalQueryStatements;
    finalQueryStatements +=
        getQueryFromFile(fs, boundFilePath, ImportDBConstants::SCHEMA_NAME, clientContext);
    // replace the path in copy from statement with the bound path
    auto copyQuery =
        getQueryFromFile(fs, boundFilePath, ImportDBConstants::COPY_NAME, clientContext);
    if (!copyQuery.empty()) {
        auto parsedStatements = Parser::parseQuery(copyQuery, clientContext);
        for (auto& parsedStatement : parsedStatements) {
            KU_ASSERT(parsedStatement->getStatementType() == StatementType::COPY_FROM);
            auto& copyFromStatement = parsedStatement->constCast<CopyFrom>();
            KU_ASSERT(copyFromStatement.getSource()->type == common::ScanSourceType::FILE);
            auto filePaths =
                copyFromStatement.getSource()->constPtrCast<FileScanSource>()->filePaths;
            KU_ASSERT(filePaths.size() == 1);
            auto fileTypeInfo = bindFileTypeInfo(filePaths);
            std::string query;
            auto copyFilePath = getCopyFilePath(boundFilePath, filePaths[0]);
            auto columnNames = getColumnNamesToCopy(copyFromStatement);
            if (fileTypeInfo.fileType == FileType::CSV) {
                auto csvConfig = CSVReaderConfig::construct(
                    bindParsingOptions(copyFromStatement.getParsingOptionsRef()));
                query = stringFormat("COPY {} {} FROM \"{}\" {};", copyFromStatement.getTableName(),
                    columnNames, copyFilePath, csvConfig.option.toCypher());
            } else {
                query = stringFormat("COPY {} {} FROM \"{}\";", copyFromStatement.getTableName(),
                    columnNames, copyFilePath);
            }
            finalQueryStatements += query;
        }
    }
    return std::make_unique<BoundImportDatabase>(boundFilePath, finalQueryStatements);
}

} // namespace binder
} // namespace kuzu
