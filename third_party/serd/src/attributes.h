// Copyright 2019-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#ifndef SERD_SRC_ATTRIBUTES_H
#define SERD_SRC_ATTRIBUTES_H

#if defined(__GNUC__)
#  define SERD_LOG_FUNC(fmt, arg1) __attribute__((format(printf, fmt, arg1)))
#else
#  define SERD_LOG_FUNC(fmt, arg1)
#endif

#ifdef __GNUC__
#  define SERD_MALLOC_FUNC __attribute__((malloc))
#else
#  define SERD_MALLOC_FUNC
#endif

#ifdef __GNUC__
#  define SERD_NODISCARD __attribute__((warn_unused_result))
#else
#  define SERD_NODISCARD
#endif

#endif // SERD_SRC_ATTRIBUTES_H
