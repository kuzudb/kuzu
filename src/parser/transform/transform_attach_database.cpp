#include "parser/attach_database.h"
#include "parser/transformer.h"

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformAttachDatabase(
    CypherParser::KU_AttachDatabaseContext& ctx) {
    AttachInfo attachInfo;
    attachInfo.dbPath = transformStringLiteral(*ctx.StringLiteral()[0]);
    attachInfo.dbAlias = "";
    if (ctx.oC_SchemaName()) {
        attachInfo.dbAlias = transformSchemaName(*ctx.oC_SchemaName());
    }
    attachInfo.dbType = transformStringLiteral(*ctx.StringLiteral()[1]);
    return std::make_unique<AttachDatabase>(std::move(attachInfo));
}

} // namespace parser
} // namespace kuzu
