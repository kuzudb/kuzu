#include "httpfs_extension.h"

#include "httpfs.h"

namespace kuzu {
namespace httpfs {

void HttpfsExtension::load(main::Database& db) {
    db.registerFileSystem(std::make_unique<HTTPFileSystem>());
}

} // namespace httpfs
} // namespace kuzu

extern "C" {
void init(kuzu::main::Database& db) {
    kuzu::httpfs::HttpfsExtension::load(db);
}
}
