#include "processor/operator/persistent/export_db.h"

#include <fcntl.h>

#include <sstream>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"

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

static void writeCopyStatement(
    stringstream& ss, std::string tableName, ReaderConfig* boundFileInfo) {
    ss << "COPY ";
    ss << tableName << " FROM \"" << boundFileInfo->filePaths[0] << "/" << tableName;
    auto fileTypeStr = FileTypeUtils::toString(boundFileInfo->fileType);
    StringUtils::toLower(fileTypeStr);
    ss << "." << fileTypeStr;
    auto csvConfig = common::CSVReaderConfig::construct(boundFileInfo->options);
    ss << "\"" << csvConfig.option.toCypher() << std::endl;
}

std::string getSchemaCypher(main::ClientContext* clientContext, transaction::Transaction* tx) {
    stringstream ss;
    auto catalog = clientContext->getCatalog();
    for (auto& nodeTableEntry : catalog->getNodeTableEntries(tx)) {
        ss << nodeTableEntry->toCypher(clientContext) << std::endl;
    }
    for (auto& relTableEntry : catalog->getRelTableEntries(tx)) {
        ss << relTableEntry->toCypher(clientContext) << std::endl;
    }
    return ss.str();
}

std::string getMacroCypher(catalog::Catalog* catalog, transaction::Transaction* tx) {
    stringstream ss;
    for (auto macroName : catalog->getMacroNames(tx)) {
        ss << catalog->getScalarMacroFunction(macroName)->toCypher(macroName) << std::endl;
    }
    return ss.str();
}

std::string getCopyCypher(
    catalog::Catalog* catalog, transaction::Transaction* tx, ReaderConfig* boundFileInfo) {
    stringstream ss;
    for (auto& nodeTableEntry : catalog->getNodeTableEntries(tx)) {
        auto tableName = nodeTableEntry->getName();
        writeCopyStatement(ss, tableName, boundFileInfo);
    }
    for (auto& relTableEntry : catalog->getRelTableEntries(tx)) {
        auto tableName = relTableEntry->getName();
        writeCopyStatement(ss, tableName, boundFileInfo);
    }
    return ss.str();
}

bool ExportDB::getNextTuplesInternal(ExecutionContext* context) {
    // write the schema.cypher file
    writeStringStreamToFile(context->clientContext->getVFSUnsafe(),
        getSchemaCypher(context->clientContext, context->clientContext->getTx()),
        boundFileInfo.filePaths[0] + "/schema.cypher");
    // write macro.cypher file
    writeStringStreamToFile(context->clientContext->getVFSUnsafe(),
        getMacroCypher(context->clientContext->getCatalog(), context->clientContext->getTx()),
        boundFileInfo.filePaths[0] + "/macro.cypher");
    // write the copy.cypher file
    // for every table, we write COPY FROM statement
    writeStringStreamToFile(context->clientContext->getVFSUnsafe(),
        getCopyCypher(
            context->clientContext->getCatalog(), context->clientContext->getTx(), &boundFileInfo),
        boundFileInfo.filePaths[0] + "/copy.cypher");
    return false;
}
} // namespace processor
} // namespace kuzu
