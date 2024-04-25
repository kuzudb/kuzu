#pragma once
#include "catalog/catalog_content.h"

namespace kuzu {
namespace extension {

class CatalogExtension : public catalog::CatalogContent {
public:
    CatalogExtension() : CatalogContent() {}

    virtual void init() = 0;

    void invalidateCache() {
        tables = std::make_unique<catalog::CatalogSet>();
        init();
    }
};

} // namespace extension
} // namespace kuzu
