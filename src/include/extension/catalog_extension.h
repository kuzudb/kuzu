#pragma once

#include "catalog/catalog.h"

namespace kuzu {
namespace extension {

class KUZU_API CatalogExtension : public catalog::Catalog {
public:
    CatalogExtension() {}

    virtual void init(transaction::Transaction* transaction) = 0;

    void invalidateCache(transaction::Transaction* transaction);
};

} // namespace extension
} // namespace kuzu
