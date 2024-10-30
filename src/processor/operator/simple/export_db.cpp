#include "processor/operator/simple/export_db.h"

#include <sstream>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "function/scalar_macro_function.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::catalog;
using namespace kuzu::main;

namespace kuzu {
namespace processor {

using std::stringstream;

std::string ExportDBPrintInfo::toString() const {
    std::string result = "Export To: ";
    result += filePath;
    if (!options.empty()) {
        result += ",Options: ";
        auto it = options.begin();
        for (auto i = 0u; it != options.end(); ++it, ++i) {
            result += it->first + "=" + it->second.toString();
            if (i < options.size() - 1) {
                result += ", ";
            }
        }
    }
    return result;
}

void ExportDB::initGlobalStateInternal(ExecutionContext* context) {
    const auto vfs = context->clientContext->getVFSUnsafe();
    KU_ASSERT(!vfs->fileOrPathExists(boundFileInfo.filePaths[0], context->clientContext));
    vfs->createDir(boundFileInfo.filePaths[0]);
}

static void writeStringStreamToFile(ClientContext* context, const std::string& ssString,
    const std::string& path) {
    const auto fileInfo = context->getVFSUnsafe()->openFile(path,
        FileFlags::WRITE | FileFlags::CREATE_IF_NOT_EXISTS, context);
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(ssString.c_str()), ssString.size(),
        0 /* offset */);
}

static void writeCopyStatement(stringstream& ss, const TableCatalogEntry* entry,
    const ReaderConfig* boundFileInfo) {
    auto fileTypeStr = boundFileInfo->fileTypeInfo.fileTypeStr;
    StringUtils::toLower(fileTypeStr);
    const auto csvConfig = CSVReaderConfig::construct(boundFileInfo->options);
    const auto tableName = entry->getName();
    std::string columns;
    const auto numProperties = entry->getNumProperties();
    for (auto i = 0u; i < numProperties; i++) {
        auto& prop = entry->getProperty(i);
        columns += prop.getName();
        columns += i == numProperties - 1 ? "" : ",";
    }
    ss << stringFormat("COPY {} ( {} ) FROM \"{}.{}\" {};\n", tableName, columns, tableName,
        fileTypeStr, csvConfig.option.toCypher());
}

std::string getSchemaCypher(ClientContext* clientContext, Transaction* tx) {
    stringstream ss;
    const auto catalog = clientContext->getCatalog();
    for (const auto& nodeTableEntry : catalog->getNodeTableEntries(tx)) {
        ss << nodeTableEntry->toCypher(clientContext) << std::endl;
    }
    for (const auto& entry : catalog->getRelTableEntries(tx)) {
        if (catalog->tableInRelGroup(tx, entry->getTableID())) {
            continue;
        }
        ss << entry->toCypher(clientContext) << std::endl;
    }
    for (const auto& relGroupEntry : catalog->getRelTableGroupEntries(tx)) {
        ss << relGroupEntry->toCypher(clientContext) << std::endl;
    }
    for (const auto sequenceEntry : catalog->getSequenceEntries(tx)) {
        ss << sequenceEntry->toCypher(clientContext) << std::endl;
    }
    for (auto macroName : catalog->getMacroNames(tx)) {
        ss << catalog->getScalarMacroFunction(tx, macroName)->toCypher(macroName) << std::endl;
    }
    return ss.str();
}

std::string getCopyCypher(const Catalog* catalog, Transaction* tx,
    const ReaderConfig* boundFileInfo) {
    stringstream ss;
    for (const auto& nodeTableEntry : catalog->getNodeTableEntries(tx)) {
        writeCopyStatement(ss, nodeTableEntry, boundFileInfo);
    }
    for (const auto& entry : catalog->getRelTableEntries(tx)) {
        writeCopyStatement(ss, entry, boundFileInfo);
    }
    return ss.str();
}

void ExportDB::executeInternal(ExecutionContext* context) {
    const auto clientContext = context->clientContext;
    const auto tx = clientContext->getTx();
    const auto catalog = clientContext->getCatalog();
    // write the schema.cypher file
    writeStringStreamToFile(clientContext, getSchemaCypher(clientContext, tx),
        boundFileInfo.filePaths[0] + "/schema.cypher");
    // write the copy.cypher file
    // for every table, we write COPY FROM statement
    writeStringStreamToFile(clientContext, getCopyCypher(catalog, tx, &boundFileInfo),
        boundFileInfo.filePaths[0] + "/copy.cypher");
}

std::string ExportDB::getOutputMsg() {
    return "Exported database successfully.";
}

} // namespace processor
} // namespace kuzu
