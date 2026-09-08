#ifndef MOQ_RELAY_EXPORT_H
#define MOQ_RELAY_EXPORT_H

/* Each DLL owns its exports independently of the core it imports. */
#if defined(_WIN32) && !defined(MOQ_RELAY_CORE_STATIC)
# ifdef MOQ_RELAY_CORE_BUILDING
#  define MOQR_CORE_API __declspec(dllexport)
# else
#  define MOQR_CORE_API __declspec(dllimport)
# endif
#elif defined(__GNUC__) || defined(__clang__)
# define MOQR_CORE_API __attribute__((visibility("default")))
#else
# define MOQR_CORE_API
#endif

#if defined(_WIN32) && !defined(MOQ_RELAY_STATIC)
# ifdef MOQ_RELAY_BUILDING
#  define MOQR_API __declspec(dllexport)
# else
#  define MOQR_API __declspec(dllimport)
# endif
#elif defined(__GNUC__) || defined(__clang__)
# define MOQR_API __attribute__((visibility("default")))
#else
# define MOQR_API
#endif

#endif
