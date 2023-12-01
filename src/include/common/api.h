#pragma once

// Helpers
#if defined _WIN32 || defined __CYGWIN__
#define KUZU_HELPER_DLL_IMPORT __declspec(dllimport)
#define KUZU_HELPER_DLL_EXPORT __declspec(dllexport)
#define KUZU_HELPER_DLL_LOCAL
#define KUZU_HELPER_DEPRECATED __declspec(deprecated)
#else
#define KUZU_HELPER_DLL_IMPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_EXPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_LOCAL __attribute__((visibility("hidden")))
#define KUZU_HELPER_DEPRECATED __attribute__((__deprecated__))
#endif

#ifdef KUZU_STATIC_DEFINE
#define KUZU_API
#else
#ifndef KUZU_API
#ifdef KUZU_EXPORTS
/* We are building this library */
#define KUZU_API KUZU_HELPER_DLL_EXPORT
#else
/* We are using this library */
#define KUZU_API KUZU_HELPER_DLL_IMPORT
#endif
#endif
#endif

#ifndef KUZU_DEPRECATED
#define KUZU_DEPRECATED KUZU_HELPER_DEPRECATED
#endif

#ifndef KUZU_DEPRECATED_EXPORT
#define KUZU_DEPRECATED_EXPORT KUZU_API KUZU_DEPRECATED
#endif
