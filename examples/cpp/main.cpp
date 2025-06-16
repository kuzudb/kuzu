#include <filesystem>
#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    std::filesystem::remove_all("adsadsad");
    auto database = std::make_unique<Database>("adsadsad" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    printf("%s",
        connection->query("CREATE NODE TABLE doc (ID UINT64, content STRING, PRIMARY KEY (ID));")
            ->toString()
            .c_str());
    printf("%s", connection
                     ->query("COPY doc from \"/home/z473chen/kuzu/50.tsv\" (file_format='csv', "
                             "delim = '\\\\t');")
                     ->toString()
                     .c_str());
    printf("%s", connection->query("CALL CREATE_FTS_INDEX('doc', 'contentIdx', ['content']);")
                     ->toString()
                     .c_str());
}
