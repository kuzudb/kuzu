#include "parser/port_db.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformExportDatabase(
    CypherParser::KU_ExportDatabaseContext& ctx) {
    std::string filePath = transformStringLiteral(*ctx.StringLiteral());
    auto exportDB = std::make_unique<ExportDB>(std::move(filePath));
    if (ctx.kU_ParsingOptions()) {
        exportDB->setParsingOption(transformParsingOptions(*ctx.kU_ParsingOptions()));
    }
    return exportDB;
}

} // namespace parser
} // namespace kuzu
