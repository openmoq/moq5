/*
 * Unit test for examples/common/moq_example_args.h — trailing-option parsing
 * for the service examples (security report finding 8: TLS verify-on default).
 * Pure C, no libmoq deps; run under CTest.
 */
#include "../moq_example_args.h"

#include <stdio.h>

static int failures = 0;
#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
                   failures++; } \
} while (0)

/* argv[0]=prog, argv[1]=url, argv[2]=ns are the fixed head; options start at 3. */
static moq_example_optargs_t parse(int argc, char **argv)
{
    return moq_example_parse_optargs(argc, argv, 3, "video");
}

static void test_default_verify_on(void)
{
    char *argv[] = { "prog", "moqt://h:4433", "demo" };
    moq_example_optargs_t a = parse(3, argv);
    CHECK(a.ok, "no trailing args parses ok");
    CHECK(a.insecure_skip_verify == false, "DEFAULT is verify-on (insecure=false)");
    CHECK(a.track && strcmp(a.track, "video") == 0, "default track preserved");
}

static void test_flag_flips_only_insecure(void)
{
    char *argv[] = { "prog", "moqt://h:4433", "demo", "--insecure-skip-verify" };
    moq_example_optargs_t a = parse(4, argv);
    CHECK(a.ok, "flag alone parses ok");
    CHECK(a.insecure_skip_verify == true, "flag sets insecure bit");
    CHECK(a.track && strcmp(a.track, "video") == 0,
          "flag does not disturb the default track");
}

static void test_track_preserved(void)
{
    char *argv[] = { "prog", "moqt://h:4433", "demo", "cam0" };
    moq_example_optargs_t a = parse(4, argv);
    CHECK(a.ok, "positional track parses ok");
    CHECK(a.insecure_skip_verify == false, "track alone leaves verify ON");
    CHECK(a.track && strcmp(a.track, "cam0") == 0, "explicit track captured");
}

static void test_either_order(void)
{
    char *before[] = { "prog", "u", "ns", "--insecure-skip-verify", "cam0" };
    moq_example_optargs_t a = parse(5, before);
    CHECK(a.ok && a.insecure_skip_verify && strcmp(a.track, "cam0") == 0,
          "flag BEFORE track: both parsed");

    char *after[] = { "prog", "u", "ns", "cam0", "--insecure-skip-verify" };
    moq_example_optargs_t b = parse(5, after);
    CHECK(b.ok && b.insecure_skip_verify && strcmp(b.track, "cam0") == 0,
          "flag AFTER track: both parsed");
}

static void test_unknown_flag_rejected(void)
{
    char *argv[] = { "prog", "u", "ns", "--bogus" };
    moq_example_optargs_t a = parse(4, argv);
    CHECK(!a.ok, "unknown flag is a usage error (not silently accepted)");
}

static void test_extra_positional_rejected(void)
{
    char *argv[] = { "prog", "u", "ns", "cam0", "cam1" };
    moq_example_optargs_t a = parse(5, argv);
    CHECK(!a.ok, "a second positional is a usage error");
}

static void test_unknown_flag_does_not_leak_insecure(void)
{
    /* A rejected parse must not have silently turned verification off. */
    char *argv[] = { "prog", "u", "ns", "--insecure-skip-verifyX" };
    moq_example_optargs_t a = parse(4, argv);
    CHECK(!a.ok, "near-miss flag name is rejected, not treated as the flag");
    CHECK(a.insecure_skip_verify == false, "rejected parse leaves verify ON");
}

int main(void)
{
    test_default_verify_on();
    test_flag_flips_only_insecure();
    test_track_preserved();
    test_either_order();
    test_unknown_flag_rejected();
    test_extra_positional_rejected();
    test_unknown_flag_does_not_leak_insecure();
    if (failures) { fprintf(stderr, "%d check(s) failed\n", failures); return 1; }
    printf("ok\n");
    return 0;
}
