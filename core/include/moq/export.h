#ifndef MOQ_EXPORT_H
#define MOQ_EXPORT_H

#if defined(_WIN32) && !defined(MOQ_STATIC)
#  ifdef MOQ_BUILDING
#    define MOQ_API __declspec(dllexport)
#  else
#    define MOQ_API __declspec(dllimport)
#  endif
#elif defined(__GNUC__) || defined(__clang__)
#  define MOQ_API __attribute__((visibility("default")))
#else
#  define MOQ_API
#endif

#if defined(_WIN32) && !defined(MOQ_STATIC)
#  ifdef MOQ_SIM_BUILDING
#    define MOQ_SIM_API __declspec(dllexport)
#  else
#    define MOQ_SIM_API __declspec(dllimport)
#  endif
#elif defined(__GNUC__) || defined(__clang__)
#  define MOQ_SIM_API __attribute__((visibility("default")))
#else
#  define MOQ_SIM_API
#endif

/* The picoquic adapter library's own exports. A separate producer macro,
 * because code that builds the adapter still imports the core's MOQ_API
 * functions. */
#if defined(_WIN32) && !defined(MOQ_STATIC)
#  ifdef MOQ_PQ_BUILDING
#    define MOQ_PQ_API __declspec(dllexport)
#  else
#    define MOQ_PQ_API __declspec(dllimport)
#  endif
#elif defined(__GNUC__) || defined(__clang__)
#  define MOQ_PQ_API __attribute__((visibility("default")))
#else
#  define MOQ_PQ_API
#endif

#endif /* MOQ_EXPORT_H */
