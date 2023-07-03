// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#ifndef SERD_SRC_URI_UTILS_H
#define SERD_SRC_URI_UTILS_H

#include "serd.h"

#include "string_utils.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

typedef struct {
  size_t shared;
  size_t root;
} SlashIndexes;

static inline bool
chunk_equals(const SerdChunk* a, const SerdChunk* b)
{
  return a->len == b->len &&
         !strncmp((const char*)a->buf, (const char*)b->buf, a->len);
}

static inline size_t
uri_path_len(const SerdURI* uri)
{
  return uri->path_base.len + uri->path.len;
}

static inline uint8_t
uri_path_at(const SerdURI* uri, size_t i)
{
  return (i < uri->path_base.len) ? uri->path_base.buf[i]
                                  : uri->path.buf[i - uri->path_base.len];
}

/**
   Return the index of the last slash shared with the root, or `SIZE_MAX`.

   The index of the next slash found in the root is also returned, so the two
   can be compared to determine if the URI is within the root (if the shared
   slash is the last in the root, then the URI is a child of the root,
   otherwise it may merely share some leading path components).
*/
static inline SERD_PURE_FUNC SlashIndexes
uri_rooted_index(const SerdURI* uri, const SerdURI* root)
{
  SlashIndexes indexes = {SIZE_MAX, SIZE_MAX};

  if (!root || !root->scheme.len ||
      !chunk_equals(&root->scheme, &uri->scheme) ||
      !chunk_equals(&root->authority, &uri->authority)) {
    return indexes;
  }

  const size_t path_len = uri_path_len(uri);
  const size_t root_len = uri_path_len(root);
  const size_t min_len  = path_len < root_len ? path_len : root_len;
  for (size_t i = 0; i < min_len; ++i) {
    const uint8_t u = uri_path_at(uri, i);
    const uint8_t r = uri_path_at(root, i);

    if (u == r) {
      if (u == '/') {
        indexes.root = indexes.shared = i;
      }
    } else {
      for (size_t j = i; j < root_len; ++j) {
        if (uri_path_at(root, j) == '/') {
          indexes.root = j;
          break;
        }
      }

      return indexes;
    }
  }

  return indexes;
}

/** Return true iff `uri` shares path components with `root` */
static inline SERD_PURE_FUNC bool
uri_is_related(const SerdURI* uri, const SerdURI* root)
{
  return uri_rooted_index(uri, root).shared != SIZE_MAX;
}

/** Return true iff `uri` is within the base of `root` */
static inline SERD_PURE_FUNC bool
uri_is_under(const SerdURI* uri, const SerdURI* root)
{
  const SlashIndexes indexes = uri_rooted_index(uri, root);
  return indexes.shared && indexes.shared != SIZE_MAX &&
         indexes.shared == indexes.root;
}

static inline bool
is_uri_scheme_char(const int c)
{
  switch (c) {
  case ':':
  case '+':
  case '-':
  case '.':
    return true;
  default:
    return is_alpha(c) || is_digit(c);
  }
}

#endif // SERD_SRC_URI_UTILS_H
