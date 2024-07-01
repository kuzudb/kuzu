#include <fcntl.h>

#include "binder/binder.h"
#include "binder/bound_import_database.h"
#include "common/cast.h"
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
    auto fileInfo = vfs->openFile(filePath, O_RDONLY
#ifdef _WIN32
                                                | _O_BINARY
#endif
    );
    auto fsize = fileInfo->getFileSize();
    auto buffer = std::make_unique<char[]>(fsize);
    fileInfo->readFile(buffer.get(), fsize);
    return std::string(buffer.get(), fsize);
}

std::unique_ptr<BoundStatement> Binder::bindImportDatabaseClause(const Statement& statement) {
    auto& importDatabaseStatement = ku_dynamic_cast<const Statement&, const ImportDB&>(statement);
    auto boundFilePath = importDatabaseStatement.getFilePath();
    auto fs = clientContext->getVFSUnsafe();
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
        auto parsedStatements = Parser::parseQuery(copyQuery);
        for (auto& parsedStatement : parsedStatements) {
            KU_ASSERT(parsedStatement->getStatementType() == StatementType::COPY_FROM);
            auto copyFromStatement =
                ku_dynamic_cast<const Statement*, const CopyFrom*>(parsedStatement.get());
            KU_ASSERT(copyFromStatement->getSource()->type == common::ScanSourceType::FILE);
            auto filePaths = ku_dynamic_cast<parser::BaseScanSource*, parser::FileScanSource*>(
                copyFromStatement->getSource())
                                 ->filePaths;
            KU_ASSERT(filePaths.size() == 1);
            auto fileType = bindFileType(filePaths);
            auto copyFilePath = boundFilePath + "/" + filePaths[0];
            std::string columns;
            std::string delimiter = "";
            for (auto& column : copyFromStatement->getColumnNames()) {
                columns += delimiter;
                columns += column;
                if (delimiter == "") {
                    delimiter = ",";
                }
            }
            std::string query;
            if (fileType == FileType::CSV) {
                auto csvConfig = CSVReaderConfig::construct(
                    bindParsingOptions(copyFromStatement->getParsingOptionsRef()));
                query = stringFormat("COPY {} ( {} ) FROM \"{}\" {};",
                    copyFromStatement->getTableName(), columns, copyFilePath,
                    csvConfig.option.toCypher());
            } else {
                query = stringFormat("COPY {} ( {} ) FROM \"{}\";",
                    copyFromStatement->getTableName(), columns, copyFilePath);
            }
            finalQueryStatements += query;
        }
    }
    return std::make_unique<BoundImportDatabase>(boundFilePath, finalQueryStatements);
}

} // namespace binder
} // namespace kuzu
