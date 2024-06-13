#include "processor/operator/simple/export_db.h"

#include <fcntl.h>

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

static void writeStringStreamToFile(VirtualFileSystem* vfs, std::string ssString,
    const std::string& path) {
    auto fileInfo = vfs->openFile(path, O_WRONLY | O_CREAT);
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(ssString.c_str()), ssString.size(),
        0 /* offset */);
}

static void writeCopyStatement(stringstream& ss, TableCatalogEntry* entry,
    ReaderConfig* boundFileInfo) {
    auto fileTypeStr = FileTypeUtils::toString(boundFileInfo->fileType);
    StringUtils::toLower(fileTypeStr);
    auto csvConfig = common::CSVReaderConfig::construct(boundFileInfo->options);
    auto tableName = entry->getName();
    std::string columns;
    for (auto i = 0u; i < entry->getNumProperties(); i++) {
        auto& prop = entry->getPropertiesRef()[i];
        columns += prop.getName();
        columns += i == entry->getNumProperties() - 1 ? "" : ",";
    }
    ss << stringFormat("COPY {} ( {} ) FROM \"{}.{}\" {};\n", tableName, columns, tableName,
        fileTypeStr, csvConfig.option.toCypher());
}

std::string getSchemaCypher(ClientContext* clientContext, Transaction* tx, std::string& extraMsg) {
    stringstream ss;
    auto catalog = clientContext->getCatalog();
    for (auto& nodeTableEntry : catalog->getNodeTableEntries(tx)) {
        ss << nodeTableEntry->toCypher(clientContext) << std::endl;
    }
    for (auto& entry : catalog->getRelTableEntries(tx)) {
        if (catalog->tableInRelGroup(tx, entry->getTableID()) ||
            catalog->tableInRDFGraph(tx, entry->getTableID())) {
            continue;
        }
        ss << entry->toCypher(clientContext) << std::endl;
    }
    for (auto& relGroupEntry : catalog->getRelTableGroupEntries(tx)) {
        ss << relGroupEntry->toCypher(clientContext) << std::endl;
    }
    if (catalog->getRdfGraphEntries(tx).size() != 0) {
        extraMsg = " But we skip exporting RDF graphs.";
    }
    for (auto sequenceEntry : catalog->getSequenceEntries(tx)) {
        ss << sequenceEntry->toCypher(clientContext) << std::endl;
    }
    for (auto macroName : catalog->getMacroNames(tx)) {
        ss << catalog->getScalarMacroFunction(tx, macroName)->toCypher(macroName) << std::endl;
    }
    return ss.str();
}

std::string getCopyCypher(Catalog* catalog, Transaction* tx, ReaderConfig* boundFileInfo) {
    stringstream ss;
    for (auto& nodeTableEntry : catalog->getNodeTableEntries(tx)) {
        if (catalog->tableInRDFGraph(tx, nodeTableEntry->getTableID())) {
            continue;
        }
        writeCopyStatement(ss, nodeTableEntry, boundFileInfo);
    }
    for (auto& entry : catalog->getRelTableEntries(tx)) {
        if (catalog->tableInRDFGraph(tx, entry->getTableID())) {
            continue;
        }
        writeCopyStatement(ss, entry, boundFileInfo);
    }
    return ss.str();
}

void ExportDB::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto fs = clientContext->getVFSUnsafe();
    auto tx = clientContext->getTx();
    auto catalog = clientContext->getCatalog();
    // write the schema.cypher file
    writeStringStreamToFile(fs, getSchemaCypher(clientContext, tx, extraMsg),
        boundFileInfo.filePaths[0] + "/schema.cypher");
    // write the copy.cypher file
    // for every table, we write COPY FROM statement
    writeStringStreamToFile(fs, getCopyCypher(catalog, tx, &boundFileInfo),
        boundFileInfo.filePaths[0] + "/copy.cypher");
}

std::string ExportDB::getOutputMsg() {
    return "Exported database successfully." + extraMsg;
}

} // namespace processor
} // namespace kuzu
