#include <filesystem>
#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    std::filesystem::remove_all("adsadsad");
    auto database = std::make_unique<Database>("adsadsad" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    auto result =
        connection->query("CREATE NODE TABLE doc (ID UINT64, content STRING, PRIMARY KEY (ID));");
    if (!result) {
        throw "Create table failed";
    }
    result = connection->query(
        "COPY doc from \"/home/z473chen/kuzu/50.tsv\" (file_format='csv', delim = '\\\\t');");
    if (!result) {
        throw "Copy failed";
    }
    result = connection->query("CALL CREATE_FTS_INDEX('doc', 'contentIdx', ['content']);");
    if (!result) {
        throw "create fts failed";
    }
}
