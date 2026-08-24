/*
 * moq_example_args.h — trailing-option parsing shared by LibMoQ service
 * examples. Example support ONLY: header-only, not installed, not public API.
 *
 * The service examples take a fixed head (`<url> <namespace>`) followed by an
 * optional track name and an optional `--insecure-skip-verify` flag, in EITHER
 * order. TLS certificate verification is ON by default; the flag disables it and
 * is intended for local/self-signed testing only. This helper keeps that
 * posture consistent and makes it unit-testable without touching example main().
 */
#ifndef MOQ_EXAMPLE_ARGS_H
#define MOQ_EXAMPLE_ARGS_H

#include <stdbool.h>
#include <string.h>

typedef struct {
    const char *track;             /* the caller's default unless overridden */
    bool insecure_skip_verify;     /* false => TLS verification ON (default) */
    bool ok;                       /* false => usage error; caller prints usage */
} moq_example_optargs_t;

/*
 * Parse argv[start..argc) as the optional trailing arguments:
 *   --insecure-skip-verify   sets insecure_skip_verify = true
 *   <track>                  one positional; overrides `default_track`
 * The flag and the track may appear in either order. `ok` is false (a usage
 * error) for an unknown flag (any argument beginning with '-' that is not the
 * known flag) or a second positional -- unknown extras are NOT silently
 * accepted. Defaults: insecure_skip_verify = false, track = default_track.
 */
static inline moq_example_optargs_t
moq_example_parse_optargs(int argc, char **argv, int start,
                          const char *default_track)
{
    moq_example_optargs_t a;
    a.track = default_track;
    a.insecure_skip_verify = false;    /* TLS verification ON by default */
    a.ok = true;
    bool track_set = false;
    for (int i = start; i < argc; i++) {
        const char *arg = argv[i];
        if (strcmp(arg, "--insecure-skip-verify") == 0) {
            a.insecure_skip_verify = true;
        } else if (arg[0] == '-') {
            a.ok = false;              /* unknown flag */
        } else if (!track_set) {
            a.track = arg;
            track_set = true;
        } else {
            a.ok = false;              /* extra positional argument */
        }
    }
    return a;
}

#endif /* MOQ_EXAMPLE_ARGS_H */
