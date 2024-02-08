#include "processor/operator/persistent/export_db.h"

#include <fcntl.h>

#include <sstream>

#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

using std::stringstream;

static void writeStringStreamToFile(
    VirtualFileSystem* vfs, stringstream& ss, const std::string& path) {
    auto ss_string = ss.str();
    auto fileInfo = vfs->openFile(path, O_WRONLY | O_CREAT);
    fileInfo->writeFile(
        reinterpret_cast<const uint8_t*>(ss_string.c_str()), ss_string.size(), 0 /* offset */);
}

static void writeCopyStatement(stringstream& ss, std::string tableName, std::string filePath) {
    ss << "COPY ";
    ss << tableName << " FROM \"" << filePath << "/" << tableName << ".csv";
    ss << "\";" << std::endl;
}

bool ExportDB::getNextTuplesInternal(ExecutionContext* context) {
    // write the copy.cypher file
    stringstream ss;
    for (auto tableCypher : tableCyphers) {
        ss << tableCypher << std::endl;
    }
    writeStringStreamToFile(
        context->clientContext->getVFSUnsafe(), ss, filePath + "/schema.cypher");
    // write macro.cypher file
    stringstream macro;
    for (auto macroCypher : macroCyphers) {
        macro << macroCypher << std::endl;
    }
    writeStringStreamToFile(
        context->clientContext->getVFSUnsafe(), macro, filePath + "/macro.cypher");
    // write the copy.cypher file
    // for every table, we write COPY FROM statement
    stringstream load_ss;
    for (auto tableName : tableNames) {
        writeCopyStatement(load_ss, tableName, filePath);
    }
    writeStringStreamToFile(
        context->clientContext->getVFSUnsafe(), load_ss, filePath + "/copy.cypher");
    return false;
}
} // namespace processor
} // namespace kuzu
