#include "processor/operator/persistent/export_db.h"

#include <fcntl.h>

#include <sstream>

#include "catalog/catalog.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "catalog/table_schema.h"
#include "common/file_system/virtual_file_system.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

using std::stringstream;

static void writeStringStreamToFile(
    VirtualFileSystem* vfs, std::string ss_string, const std::string& path) {
    auto fileInfo = vfs->openFile(path, O_WRONLY | O_CREAT);
    fileInfo->writeFile(
        reinterpret_cast<const uint8_t*>(ss_string.c_str()), ss_string.size(), 0 /* offset */);
}

static void writeCopyStatement(stringstream& ss, std::string tableName, std::string filePath,
    std::string copyOption, common::FileType fileType) {
    ss << "COPY ";
    ss << tableName << " FROM \"" << filePath << "/" << tableName;
    if (fileType == FileType::CSV) {
        ss << ".csv";
    } else {
        ss << ".parquet";
    }
    ss << "\"" << copyOption << std::endl;
}

std::string getSchemaCypher(catalog::Catalog* catalog) {
    stringstream ss;
    for (auto& schema : catalog->getNodeTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto nodeSchema = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(schema);
        ss << nodeSchema->toCypher(schema->getName(), schema->getName()) << std::endl;
    }
    for (auto& schema : catalog->getRelTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto relSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(schema);
        auto srcTableName =
            catalog->getTableName(&DUMMY_READ_TRANSACTION, relSchema->getSrcTableID());
        auto dstTableName =
            catalog->getTableName(&DUMMY_READ_TRANSACTION, relSchema->getDstTableID());
        ss << relSchema->toCypher(srcTableName, dstTableName) << std::endl;
    }
    return ss.str();
}

std::string getMacroCypher(catalog::Catalog* catalog) {
    stringstream ss;
    for (auto macroName : catalog->getMacroNames(&DUMMY_READ_TRANSACTION)) {
        ss << catalog->getScalarMacroFunction(macroName)->toCypher(macroName) << std::endl;
    }
    return ss.str();
}

std::string getCopyCypher(catalog::Catalog* catalog, std::string filePath, std::string copyOption,
    common::FileType fileType) {
    stringstream ss;
    for (auto& schema : catalog->getNodeTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto tableName = schema->getName();
        writeCopyStatement(ss, tableName, filePath, copyOption, fileType);
    }
    for (auto& schema : catalog->getRelTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto tableName = schema->getName();
        writeCopyStatement(ss, tableName, filePath, copyOption, fileType);
    }
    return ss.str();
}

bool ExportDB::getNextTuplesInternal(ExecutionContext* context) {
    // write the schema.cypher file
    writeStringStreamToFile(context->clientContext->getVFSUnsafe(),
        getSchemaCypher(context->clientContext->getCatalog()), filePath + "/schema.cypher");
    // write macro.cypher file
    writeStringStreamToFile(context->clientContext->getVFSUnsafe(),
        getMacroCypher(context->clientContext->getCatalog()), filePath + "/macro.cypher");
    // write the copy.cypher file
    // for every table, we write COPY FROM statement
    auto copyToOptionStr = fileType == FileType::CSV ? copyToOption.toCypher() : "";
    writeStringStreamToFile(context->clientContext->getVFSUnsafe(),
        getCopyCypher(context->clientContext->getCatalog(), filePath, copyToOptionStr, fileType),
        filePath + "/copy.cypher");
    return false;
}
} // namespace processor
} // namespace kuzu
