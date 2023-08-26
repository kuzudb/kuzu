// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#ifndef SERD_SRC_SERD_INTERNAL_H
#define SERD_SRC_SERD_INTERNAL_H

#include "serd.h"

#include <stdio.h>

#define NS_XSD "http://www.w3.org/2001/XMLSchema#"
#define NS_RDF "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

#define SERD_PAGE_SIZE 4096

#ifndef MIN
#  define MIN(a, b) (((a) < (b)) ? (a) : (b))
#endif

/* Error reporting */

static inline void
serd_error(SerdErrorSink error_sink, void* handle, const SerdError* e)
{
  if (error_sink) {
    error_sink(handle, e);
  } else {
    fprintf(stderr, "error: %s:%u:%u: ", e->filename, e->line, e->col);
    vfprintf(stderr, e->fmt, *e->args);
  }
}

#endif // SERD_SRC_SERD_INTERNAL_H
