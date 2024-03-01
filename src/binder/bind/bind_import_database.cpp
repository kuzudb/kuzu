#include "binder/binder.h"
#include "binder/copy/bound_import_database.h"
#include "common/exception/binder.h"
#include "parser/port_db.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::string getFilePath(
    common::VirtualFileSystem* vfs, const std::string boundFilePath, const std::string fileName) {
    auto filePath = vfs->joinPath(boundFilePath, fileName);
    if (!vfs->fileOrPathExists(filePath)) {
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
    if (!vfs->fileOrPathExists(boundFilePath)) {
        throw BinderException(stringFormat("Directory {} does not exist.", boundFilePath));
    }
    std::string finalQueryStatements;
    finalQueryStatements += getFilePath(vfs, boundFilePath, ImportDBConstants::SCHEMA_NAME);
    finalQueryStatements += getFilePath(vfs, boundFilePath, ImportDBConstants::COPY_NAME);
    finalQueryStatements += getFilePath(vfs, boundFilePath, ImportDBConstants::MACRO_NAME);
    return std::make_unique<BoundImportDatabase>(boundFilePath, finalQueryStatements);
}
} // namespace binder
} // namespace kuzu
