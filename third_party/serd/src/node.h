// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#ifndef SERD_SRC_NODE_H
#define SERD_SRC_NODE_H

#include "serd.h"

#include <stddef.h>

struct SerdNodeImpl {
  size_t        n_bytes; /**< Size in bytes (not including null) */
  SerdNodeFlags flags;   /**< Node flags (e.g. string properties) */
  SerdType      type;    /**< Node type */
};

static inline char* SERD_NONNULL
serd_node_buffer(SerdNode* SERD_NONNULL node)
{
  return (char*)(node + 1);
}

static inline const char* SERD_NONNULL
serd_node_buffer_c(const SerdNode* SERD_NONNULL node)
{
  return (const char*)(node + 1);
}

SerdNode* SERD_ALLOCATED
serd_node_malloc(size_t n_bytes, SerdNodeFlags flags, SerdType type);

void
serd_node_set(SerdNode* SERD_NULLABLE* SERD_NONNULL dst,
              const SerdNode* SERD_NULLABLE         src);

#endif // SERD_SRC_NODE_H
