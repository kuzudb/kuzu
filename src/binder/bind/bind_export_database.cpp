#include "binder/binder.h"
#include "binder/copy/bound_export_database.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"
#include "parser/db.h"
#include "parser/parser.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindExportDatabaseClause(const Statement& statement) {
    auto& exportDatabaseStatement = ku_dynamic_cast<const Statement&, const ExportDB&>(statement);
    auto boundFilePath = exportDatabaseStatement.getFilePath();
    // TODO(Jiamin): add more supported file types
    auto fileType = FileType::CSV;
    auto csvConfig = CSVReaderConfig::construct(
        bindParsingOptions(exportDatabaseStatement.getParsingOptionsRef()));
    // try to create the directory, if it doesn't exist yet
    if (!vfs->fileOrPathExists(boundFilePath)) {
        vfs->createDir(boundFilePath);
    } else {
        // TODO(Jimain): add here before merge
        // throw BinderException("incorrect directory!");
    }
    return std::make_unique<BoundExportDatabase>(boundFilePath, fileType, csvConfig.option.copy());
}
} // namespace binder
} // namespace kuzu
