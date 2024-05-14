#pragma once
#include "catalog/catalog.h"

namespace kuzu {
namespace extension {

class CatalogExtension : public catalog::Catalog {
public:
    CatalogExtension() : Catalog() {}

    virtual void init() = 0;

    void invalidateCache();
};

} // namespace extension
} // namespace kuzu
