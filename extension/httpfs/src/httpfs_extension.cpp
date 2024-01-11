#include "httpfs_extension.h"

#include "common/types/types.h"
#include "common/types/value/value.h"
#include "httpfs.h"

namespace kuzu {
namespace httpfs {

void HttpfsExtension::load(main::Database& db) {
    db.registerFileSystem(std::make_unique<HTTPFileSystem>());
    db.addExtensionOption("s3_access_key_id", common::LogicalTypeID::STRING, common::Value{""});
}

} // namespace httpfs
} // namespace kuzu

extern "C" {
void init(kuzu::main::Database& db) {
    kuzu::httpfs::HttpfsExtension::load(db);
}
}
