#include "config.h"

#include "admin_listen.h"
#include "info_doc.h"

#include "broker.h"
#include "../admin/moqr_admin.h"

#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "snapshot.h"

#include <moqrelay/capacity.h>

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Vendored single-header JSON parser (same one media/msf ships; the CLI
 * points its include path at that vendor directory — one copy in tree). */
#include "json.h"

#define CFG_MAX_ENTRIES  (1u << 20)          /* per-pool sanity cap      */
#define CFG_MAX_BYTES    (UINT64_C(1) << 40) /* per-budget sanity cap    */

static void
cfg_err(char *err, size_t err_len, const char *msg)
{
    if (err != NULL && err_len > 0) {
        snprintf(err, err_len, "%s", msg);
    }
}

/* The same fixed message with an array index appended. cfg_err takes a literal
 * message and no format precisely so a rejected value cannot be echoed back at
 * the operator; the index is a position this parser counted, not anything the
 * document contained. */
static void
cfg_err_at(char *err, size_t err_len, const char *msg, size_t index)
{
    if (err != NULL && err_len > 0) {
        snprintf(err, err_len, "%s at entry %zu", msg, index);
    }
}

static bool
num_u64(struct json_value_s *v, uint64_t *out)
{
    struct json_number_s *n = json_value_as_number(v);
    if (n == NULL) {
        return false;
    }
    /* strtoull silently accepts a leading '-' (wrapping) and skips leading
     * whitespace; reject anything but a plain non-negative integer, and
     * catch overflow via ERANGE. */
    const char *p = n->number;
    if (!isdigit((unsigned char)p[0])) {
        return false;   /* no sign, no space, no leading dot */
    }
    errno = 0;
    char *end = NULL;
    unsigned long long x = strtoull(p, &end, 10);
    if (end == NULL || *end != '\0' || errno == ERANGE) {
        return false;
    }
    *out = (uint64_t)x;
    return true;
}

static bool
copy_string(struct json_value_s *v, char *dst, size_t cap)
{
    struct json_string_s *s = json_value_as_string(v);
    if (s == NULL || s->string_size >= cap) {
        return false;
    }
    memcpy(dst, s->string, s->string_size);
    dst[s->string_size] = '\0';
    return true;
}

/*
 * WebTransport string values.
 *
 * Everything this object stores is consumed as a C string -- a host, a path,
 * a credential path, a serialized Origin -- so a value carrying an embedded
 * NUL would be silently truncated at the boundary that consumes it, and the
 * operator would be running a configuration they did not write. These helpers
 * refuse that instead. They are deliberately scoped to this object: the shared
 * copy_string keeps its existing semantics everywhere else.
 */
static bool
wt_str(struct json_value_s *v, const char **p, size_t *n)
{
    struct json_string_s *s = json_value_as_string(v);
    if (s == NULL || memchr(s->string, '\0', s->string_size) != NULL) {
        return false;
    }
    *p = s->string;
    *n = s->string_size;
    return true;
}

static bool
wt_copy(struct json_value_s *v, char *dst, size_t cap)
{
    const char *p = NULL;
    size_t n = 0;
    if (!wt_str(v, &p, &n) || n >= cap) {
        return false;
    }
    memcpy(dst, p, n);
    dst[n] = '\0';
    return true;
}

/* A closed-vocabulary token matches only on complete decoded length AND bytes,
 * so a value with a trailing NUL is not the token it is a prefix of. */
static bool
wt_val_is(const char *p, size_t n, const char *lit)
{
    size_t m = strlen(lit);
    return n == m && memcmp(p, lit, m) == 0;
}

static moqr_result_t
parse_listener(struct json_object_s *o, moqr_cli_config_t *out,
               char *err, size_t err_len)
{
    bool have_port = false;
    for (struct json_object_element_s *e = o->start; e != NULL;
         e = e->next) {
        const char *k = e->name->string;
        if (strcmp(k, "transport") == 0) {
            char t[32];
            if (!copy_string(e->value, t, sizeof(t)) ||
                strcmp(t, "msquic") != 0) {
                cfg_err(err, err_len,
                        "listener.transport: only \"msquic\" is supported");
                return MOQR_ERR_INVAL;
            }
        } else if (strcmp(k, "host") == 0) {
            if (!copy_string(e->value, out->host, sizeof(out->host))) {
                cfg_err(err, err_len, "listener.host: invalid string");
                return MOQR_ERR_INVAL;
            }
        } else if (strcmp(k, "port") == 0) {
            uint64_t p = 0;
            if (!num_u64(e->value, &p) || p < 1 || p > 65535) {
                cfg_err(err, err_len, "listener.port: need 1..65535");
                return MOQR_ERR_INVAL;
            }
            out->port = (int)p;
            have_port = true;
        } else if (strcmp(k, "cert") == 0) {
            if (!copy_string(e->value, out->cert, sizeof(out->cert))) {
                cfg_err(err, err_len, "listener.cert: invalid string");
                return MOQR_ERR_INVAL;
            }
        } else if (strcmp(k, "key") == 0) {
            if (!copy_string(e->value, out->key, sizeof(out->key))) {
                cfg_err(err, err_len, "listener.key: invalid string");
                return MOQR_ERR_INVAL;
            }
        } else if (strcmp(k, "versions") == 0) {
            /* An ORDERED, non-empty set of supported drafts, most preferred
             * first. One entry keeps the exact-version listener; several offer
             * several ALPNs on one listener and ALPN performs the version
             * negotiation (draft-18 Section 3.1/3.1.1). Everything malformed
             * fails closed: empty, over-bound, duplicate, unknown, boolean,
             * fractional or non-numeric. */
            struct json_array_s *arr = json_value_as_array(e->value);
            if (arr == NULL || arr->length == 0) {
                cfg_err(err, err_len,
                        "listener.versions: a non-empty array is required");
                return MOQR_ERR_INVAL;
            }
            if (arr->length > MOQR_CLI_MAX_VERSIONS) {
                cfg_err(err, err_len, "listener.versions: too many entries");
                return MOQR_ERR_INVAL;
            }
            size_t n = 0;
            for (struct json_array_element_s *ae = arr->start; ae != NULL;
                 ae = ae->next) {
                uint64_t v = 0;
                if (!num_u64(ae->value, &v) || (v != 16 && v != 18)) {
                    cfg_err(err, err_len,
                            "listener.versions: only 16 and 18 are known");
                    return MOQR_ERR_INVAL;
                }
                moq_version_t mv = (v == 16) ? MOQ_VERSION_DRAFT_16
                                             : MOQ_VERSION_DRAFT_18;
                for (size_t j = 0; j < n; j++) {
                    if (out->versions[j] == mv) {
                        cfg_err(err, err_len,
                                "listener.versions: duplicate entry");
                        return MOQR_ERR_INVAL;
                    }
                }
                snprintf(out->alpn_buf[n], sizeof(out->alpn_buf[n]),
                         "moqt-%" PRIu64, v);
                out->alpns[n] = out->alpn_buf[n];
                out->versions[n] = mv;
                if (n == 0) {
                    out->version = (uint32_t)v;   /* the preferred entry */
                }
                n++;
            }
            out->alpn_count = n;
            out->version_count = n;
        } else if (strcmp(k, "insecure_skip_verify") == 0) {
            if (json_value_is_true(e->value)) {
                out->insecure_skip_verify = true;
            } else if (json_value_is_false(e->value)) {
                out->insecure_skip_verify = false;
            } else {
                cfg_err(err, err_len,
                        "listener.insecure_skip_verify: need true/false");
                return MOQR_ERR_INVAL;
            }
        } else if (strcmp(k, "lanes") == 0) {
            uint64_t n = 0;
            if (!num_u64(e->value, &n) || n < 1 || n > MOQR_CLI_MAX_LANES) {
                cfg_err(err, err_len, "listener.lanes: need 1..64");
                return MOQR_ERR_INVAL;
            }
            out->lanes = (uint32_t)n;
        } else {
            cfg_err(err, err_len, "listener: unknown key");
            return MOQR_ERR_INVAL;
        }
    }
    if (!have_port) {
        cfg_err(err, err_len, "listener.port is required");
        return MOQR_ERR_INVAL;
    }
    return MOQR_OK;
}

static moqr_result_t
budget_entry_u32(struct json_value_s *v, uint32_t *dst,
                 const char *what, char *err, size_t err_len)
{
    uint64_t x = 0;
    if (!num_u64(v, &x) || x > CFG_MAX_ENTRIES) {
        snprintf(err, err_len, "budgets.%s: need 0..%u", what,
                 CFG_MAX_ENTRIES);
        return MOQR_ERR_INVAL;
    }
    *dst = (uint32_t)x;
    return MOQR_OK;
}

static moqr_result_t
parse_budgets(struct json_object_s *o, moqr_cli_config_t *out,
              char *err, size_t err_len)
{
    for (struct json_object_element_s *e = o->start; e != NULL;
         e = e->next) {
        const char *k = e->name->string;
        moqr_result_t rc = MOQR_OK;
        if (strcmp(k, "max_tracks") == 0) {
            rc = budget_entry_u32(e->value, &out->core.max_tracks, k, err,
                                  err_len);
        } else if (strcmp(k, "max_subs") == 0) {
            rc = budget_entry_u32(e->value, &out->core.max_subs, k, err,
                                  err_len);
        } else if (strcmp(k, "max_bindings") == 0) {
            rc = budget_entry_u32(e->value, &out->core.max_bindings, k, err,
                                  err_len);
        } else if (strcmp(k, "max_ns_nodes") == 0) {
            rc = budget_entry_u32(e->value, &out->core.max_ns_nodes, k, err,
                                  err_len);
        } else if (strcmp(k, "max_ns_subs") == 0) {
            rc = budget_entry_u32(e->value, &out->core.max_ns_subs, k, err,
                                  err_len);
        } else if (strcmp(k, "max_intents") == 0) {
            rc = budget_entry_u32(e->value, &out->core.max_intents, k, err,
                                  err_len);
        } else if (strcmp(k, "name_intern_bytes") == 0) {
            rc = budget_entry_u32(e->value, &out->core.name_intern_bytes, k,
                                  err, err_len);
        } else if (strcmp(k, "log_max_subgroups") == 0) {
            rc = budget_entry_u32(e->value, &out->core.log_max_subgroups, k,
                                  err, err_len);
        } else if (strcmp(k, "log_max_objects") == 0) {
            rc = budget_entry_u32(e->value,
                                  &out->core.log_max_objects_per_group, k,
                                  err, err_len);
        } else if (strcmp(k, "log_max_cursors") == 0) {
            rc = budget_entry_u32(e->value, &out->core.log_max_cursors, k,
                                  err, err_len);
        } else if (strcmp(k, "log_max_chunk_nodes") == 0) {
            rc = budget_entry_u32(e->value, &out->core.log_max_chunk_nodes, k,
                                  err, err_len);
        } else if (strcmp(k, "log") == 0) {
            struct json_object_s *lo = json_value_as_object(e->value);
            if (lo == NULL) {
                cfg_err(err, err_len, "budgets.log: need an object");
                return MOQR_ERR_INVAL;
            }
            for (struct json_object_element_s *le = lo->start;
                 le != NULL; le = le->next) {
                const char *lk = le->name->string;
                uint64_t x = 0;
                if (!num_u64(le->value, &x)) {
                    cfg_err(err, err_len, "budgets.log: need numbers");
                    return MOQR_ERR_INVAL;
                }
                if (strcmp(lk, "max_groups") == 0) {
                    if (x > CFG_MAX_ENTRIES) {
                        cfg_err(err, err_len,
                                "budgets.log.max_groups: oversized");
                        return MOQR_ERR_INVAL;
                    }
                    out->core.log_budget.max_groups = (uint32_t)x;
                } else if (strcmp(lk, "max_bytes") == 0) {
                    if (x > CFG_MAX_BYTES) {
                        cfg_err(err, err_len,
                                "budgets.log.max_bytes: oversized");
                        return MOQR_ERR_INVAL;
                    }
                    out->core.log_budget.max_bytes = x;
                } else if (strcmp(lk, "max_age_us") == 0) {
                    out->core.log_budget.max_age_us = x;
                } else {
                    cfg_err(err, err_len, "budgets.log: unknown key");
                    return MOQR_ERR_INVAL;
                }
            }
        } else if (strcmp(k, "cross_shard") == 0) {
            struct json_object_s *xo = json_value_as_object(e->value);
            if (xo == NULL) {
                cfg_err(err, err_len, "budgets.cross_shard: need an object");
                return MOQR_ERR_INVAL;
            }
            for (struct json_object_element_s *xe = xo->start; xe != NULL;
                 xe = xe->next) {
                const char *xk = xe->name->string;
                /* Key whitelist FIRST: a rejected key reports "unknown key"
                 * loudly, never a misleading type complaint. */
                uint32_t *dst = NULL;
                bool is_bytes = false;
                if (strcmp(xk, "journal_entries") == 0) {
                    dst = &out->cross_shard.journal_entries;
                } else if (strcmp(xk, "mailbox_entries") == 0) {
                    dst = &out->cross_shard.mailbox_entries;
                } else if (strcmp(xk, "demand_channel_entries") == 0) {
                    dst = &out->cross_shard.demand_channel_entries;
                } else if (strcmp(xk, "pending_demands") == 0) {
                    dst = &out->cross_shard.pending_demands;
                } else if (strcmp(xk, "subgroup_slots") == 0) {
                    dst = &out->cross_shard.subgroup_slots;
                } else if (strcmp(xk, "demand_channel_bytes") == 0) {
                    is_bytes = true;
                } else {
                    cfg_err(err, err_len,
                            "budgets.cross_shard: unknown key");
                    return MOQR_ERR_INVAL;
                }
                uint64_t x = 0;
                if (!num_u64(xe->value, &x)) {
                    cfg_err(err, err_len,
                            "budgets.cross_shard: need non-negative numbers");
                    return MOQR_ERR_INVAL;
                }
                if (is_bytes) {
                    if (x > CFG_MAX_BYTES) {
                        cfg_err(err, err_len,
                                "budgets.cross_shard.demand_channel_bytes: "
                                "oversized");
                        return MOQR_ERR_INVAL;
                    }
                    out->cross_shard.demand_channel_bytes = x;
                } else {
                    if (x > CFG_MAX_ENTRIES) {
                        cfg_err(err, err_len,
                                "budgets.cross_shard: entry count oversized");
                        return MOQR_ERR_INVAL;
                    }
                    *dst = (uint32_t)x;
                }
            }
        } else {
            cfg_err(err, err_len, "budgets: unknown key");
            return MOQR_ERR_INVAL;
        }
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    return MOQR_OK;
}

static moqr_result_t
parse_telemetry(struct json_object_s *o, moqr_cli_config_t *out, char *err,
                size_t err_len)
{
    for (struct json_object_element_s *e = o->start; e != NULL;
         e = e->next) {
        const char *k = e->name->string;
        if (strcmp(k, "trace_ring_records") == 0) {
            uint64_t x = 0;
            if (!num_u64(e->value, &x) || x > CFG_MAX_ENTRIES) {
                snprintf(err, err_len,
                         "telemetry.trace_ring_records: need 0..%u",
                         CFG_MAX_ENTRIES);
                return MOQR_ERR_INVAL;
            }
            out->telemetry.trace_ring_records = (uint32_t)x;
        } else {
            cfg_err(err, err_len, "telemetry: unknown key");
            return MOQR_ERR_INVAL;
        }
    }
    return MOQR_OK;
}

static bool
auth_action_from_str(const char *s, moqr_auth_action_t *out)
{
    static const struct {
        const char        *name;
        moqr_auth_action_t val;
    } tab[] = {
        { "client_setup", MOQR_AUTH_CLIENT_SETUP },
        { "server_setup", MOQR_AUTH_SERVER_SETUP },
        { "publish_namespace", MOQR_AUTH_PUBLISH_NAMESPACE },
        { "subscribe_namespace", MOQR_AUTH_SUBSCRIBE_NAMESPACE },
        { "subscribe", MOQR_AUTH_SUBSCRIBE },
        { "request_update", MOQR_AUTH_REQUEST_UPDATE },
        { "publish", MOQR_AUTH_PUBLISH },
        { "fetch", MOQR_AUTH_FETCH },
        { "track_status", MOQR_AUTH_TRACK_STATUS },
    };
    for (size_t i = 0; i < sizeof(tab) / sizeof(tab[0]); i++) {
        if (strcmp(s, tab[i].name) == 0) {
            *out = tab[i].val;
            return true;
        }
    }
    return false;
}

/* "allow" or "deny" only — "defer" and unknowns are rejected: the toy policy
 * is ALLOW/DENY, and DEFER must never be authorable from config. */
static bool
auth_decision_from_str(const char *s, moqr_auth_decision_t *out)
{
    if (strcmp(s, "allow") == 0) {
        *out = MOQR_AUTH_ALLOW;
        return true;
    }
    if (strcmp(s, "deny") == 0) {
        *out = MOQR_AUTH_DENY;
        return true;
    }
    return false;
}

static bool
auth_reason_from_str(const char *s, moqr_auth_reason_t *out)
{
    static const struct {
        const char        *name;
        moqr_auth_reason_t val;
    } tab[] = {
        { "ok", MOQR_AUTH_REASON_OK },
        { "no_token", MOQR_AUTH_REASON_NO_TOKEN },
        { "unscoped", MOQR_AUTH_REASON_UNSCOPED },
        { "expired", MOQR_AUTH_REASON_EXPIRED },
        { "not_yet_valid", MOQR_AUTH_REASON_NOT_YET_VALID },
        { "bad_issuer", MOQR_AUTH_REASON_BAD_ISSUER },
        { "bad_audience", MOQR_AUTH_REASON_BAD_AUDIENCE },
        { "bad_signature", MOQR_AUTH_REASON_BAD_SIGNATURE },
        { "policy", MOQR_AUTH_REASON_POLICY },
    };
    for (size_t i = 0; i < sizeof(tab) / sizeof(tab[0]); i++) {
        if (strcmp(s, tab[i].name) == 0) {
            *out = tab[i].val;
            return true;
        }
    }
    return false;
}

/* Parse one toy rule object into rules[idx] / ns[idx] (both caller-owned). */
static moqr_result_t
parse_auth_rule(struct json_object_s *ro, moqr_cli_auth_t *auth, size_t idx,
                char *err, size_t err_len)
{
    moqr_auth_toy_rule_t *rule = &auth->rules[idx];
    moqr_cli_auth_ns_t   *ns = &auth->ns[idx];
    bool have_action = false;
    bool have_decision = false;
    rule->reason = MOQR_AUTH_REASON_POLICY; /* default deny reason */
    ns->count = 0;
    for (struct json_object_element_s *e = ro->start; e != NULL; e = e->next) {
        const char *k = e->name->string;
        if (strcmp(k, "action") == 0) {
            char s[32];
            moqr_auth_action_t a;
            if (!copy_string(e->value, s, sizeof(s)) ||
                !auth_action_from_str(s, &a)) {
                cfg_err(err, err_len, "auth.rules[].action: unknown action");
                return MOQR_ERR_INVAL;
            }
            rule->action = a;
            have_action = true;
        } else if (strcmp(k, "decision") == 0) {
            char s[16];
            moqr_auth_decision_t d;
            if (!copy_string(e->value, s, sizeof(s)) ||
                !auth_decision_from_str(s, &d)) {
                cfg_err(err, err_len,
                        "auth.rules[].decision: need \"allow\" or \"deny\"");
                return MOQR_ERR_INVAL;
            }
            rule->decision = d;
            have_decision = true;
        } else if (strcmp(k, "reason") == 0) {
            char s[24];
            moqr_auth_reason_t r;
            if (!copy_string(e->value, s, sizeof(s)) ||
                !auth_reason_from_str(s, &r)) {
                cfg_err(err, err_len, "auth.rules[].reason: unknown reason");
                return MOQR_ERR_INVAL;
            }
            rule->reason = r;
        } else if (strcmp(k, "namespace_prefix") == 0) {
            struct json_array_s *arr = json_value_as_array(e->value);
            if (arr == NULL || arr->length > MOQR_CLI_MAX_NS_PARTS) {
                cfg_err(err, err_len,
                        "auth.rules[].namespace_prefix: need 0..8 parts");
                return MOQR_ERR_INVAL;
            }
            size_t j = 0;
            for (struct json_array_element_s *ae = arr->start; ae != NULL;
                 ae = ae->next, j++) {
                struct json_string_s *ps = json_value_as_string(ae->value);
                if (ps == NULL || ps->string_size > MOQR_CLI_MAX_NS_PART_LEN) {
                    cfg_err(err, err_len,
                            "auth.rules[].namespace_prefix: part must be a "
                            "string of <= 64 bytes");
                    return MOQR_ERR_INVAL;
                }
                memcpy(ns->bytes[j], ps->string, ps->string_size);
                ns->parts[j].data = ns->bytes[j];
                ns->parts[j].len = ps->string_size;
            }
            ns->count = j;
        } else {
            cfg_err(err, err_len, "auth.rules[]: unknown key");
            return MOQR_ERR_INVAL;
        }
    }
    if (!have_action || !have_decision) {
        cfg_err(err, err_len,
                "auth.rules[]: action and decision are required");
        return MOQR_ERR_INVAL;
    }
    rule->ns_prefix.parts = ns->parts;
    rule->ns_prefix.count = ns->count;
    return MOQR_OK;
}

static moqr_result_t
parse_auth(struct json_object_s *o, moqr_cli_config_t *out, char *err,
           size_t err_len)
{
    moqr_cli_auth_t     *auth = &out->auth;
    bool                 have_mode = false;
    bool                 have_default = false;
    bool                 have_rules = false;
    moqr_auth_decision_t defd = MOQR_AUTH_DENY;
    struct json_array_s *rules_arr = NULL;

    for (struct json_object_element_s *e = o->start; e != NULL; e = e->next) {
        const char *k = e->name->string;
        if (strcmp(k, "mode") == 0) {
            char s[16];
            if (!copy_string(e->value, s, sizeof(s))) {
                cfg_err(err, err_len, "auth.mode: invalid string");
                return MOQR_ERR_INVAL;
            }
            if (strcmp(s, "allow_all") == 0) {
                auth->mode = MOQR_CLI_AUTH_ALLOW_ALL;
            } else if (strcmp(s, "toy") == 0) {
                auth->mode = MOQR_CLI_AUTH_TOY;
            } else {
                cfg_err(err, err_len,
                        "auth.mode: need \"allow_all\" or \"toy\"");
                return MOQR_ERR_INVAL;
            }
            have_mode = true;
        } else if (strcmp(k, "default") == 0) {
            char s[16];
            if (!copy_string(e->value, s, sizeof(s)) ||
                !auth_decision_from_str(s, &defd)) {
                cfg_err(err, err_len,
                        "auth.default: need \"allow\" or \"deny\"");
                return MOQR_ERR_INVAL;
            }
            have_default = true;
        } else if (strcmp(k, "rules") == 0) {
            rules_arr = json_value_as_array(e->value);
            if (rules_arr == NULL ||
                rules_arr->length > MOQR_CLI_MAX_AUTH_RULES) {
                cfg_err(err, err_len, "auth.rules: need an array of 0..32");
                return MOQR_ERR_INVAL;
            }
            have_rules = true;
        } else {
            cfg_err(err, err_len, "auth: unknown key");
            return MOQR_ERR_INVAL;
        }
    }
    if (!have_mode) {
        cfg_err(err, err_len, "auth.mode is required");
        return MOQR_ERR_INVAL;
    }
    if (auth->mode == MOQR_CLI_AUTH_ALLOW_ALL) {
        if (have_default || have_rules) {
            cfg_err(err, err_len,
                    "auth: default/rules are only valid with mode \"toy\"");
            return MOQR_ERR_INVAL;
        }
        return MOQR_OK;
    }
    /* mode == toy: a default is mandatory (fail-open vs fail-closed is never
     * implicit), rules are optional. */
    if (!have_default) {
        cfg_err(err, err_len, "auth.default is required for mode \"toy\"");
        return MOQR_ERR_INVAL;
    }
    size_t i = 0;
    for (struct json_array_element_s *ae =
             rules_arr != NULL ? rules_arr->start : NULL;
         ae != NULL; ae = ae->next, i++) {
        struct json_object_s *ro = json_value_as_object(ae->value);
        if (ro == NULL) {
            cfg_err(err, err_len, "auth.rules[]: need an object");
            return MOQR_ERR_INVAL;
        }
        moqr_result_t rc = parse_auth_rule(ro, auth, i, err, err_len);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    /* Wire the self-consistent toy context (borrows into auth->rules/ns). */
    auth->toy.rules = auth->rules;
    auth->toy.rule_count = i;
    auth->toy.default_decision = defd;
    auth->toy.default_reason = MOQR_AUTH_REASON_POLICY;
    return MOQR_OK;
}

/*
 * The optional WebTransport listener.
 *
 * Its own address, TLS material and lane count, plus the HTTP/3 surface a
 * browser needs: the request path, the ordered MoQ drafts offered as
 * subprotocols, and the WebTransport wire profile. Unknown keys and unknown
 * values fail closed -- a relay that silently ignored "profile": "tomorrow"
 * would come up speaking a dialect the operator did not ask for.
 */
/* Loopback, decided by parsing the literal address into its BINARY form.
 *
 * sscanf accepts spellings the kernel will not: leading whitespace, a sign, a
 * missing octet. Accepting them here defers the refusal to bind time, after the
 * config has already been called valid. inet_pton accepts exactly one spelling
 * per address, and the check then inspects the bytes rather than the text.
 *
 * A resolver is deliberately not used: it would accept "localhost", and
 * whatever that maps to today or after the next /etc/hosts edit. The v1 rule is
 * that the operator writes a literal this function recognises. IPv4-mapped
 * forms are refused as well -- one spelling per rule is what keeps the check
 * auditable. */
static bool
admin_host_is_loopback(const char *h)
{
    struct in_addr v4;
    struct in6_addr v6;

    if (h == NULL || h[0] == '\0') {
        return false;
    }
    if (strchr(h, ':') != NULL) {
        if (inet_pton(AF_INET6, h, &v6) != 1) {
            return false;
        }
        /* ::1 only. A v4-mapped or v4-compatible address is a second spelling
         * for a v4 address and is refused rather than translated. */
        return IN6_IS_ADDR_LOOPBACK(&v6) ? true : false;
    }
    /* Reject zero-padded octets before converting. Some implementations of
     * inet_pton accept "127.000.000.001", and other parsers read a leading
     * zero as octal -- so the same text can denote two different addresses
     * depending on who reads it. One spelling, or refuse. */
    {
        const char *p = h;
        while (*p != '\0') {
            if (*p == '0' && p[1] != '\0' && p[1] != '.') {
                return false;
            }
            /* advance to the start of the next octet */
            while (*p != '\0' && *p != '.') {
                p++;
            }
            if (*p == '.') {
                p++;
            }
        }
    }
    if (inet_pton(AF_INET, h, &v4) != 1) {
        return false;
    }
    /* 127.0.0.0/8. 0.0.0.0 is a wildcard, not a loopback. */
    return (ntohl(v4.s_addr) >> 24) == 127u;
}

/* Duplicate object keys are ambiguous: last-one-wins silently discards the
 * value the operator meant. Each parser below records the keys it has seen. */
static bool
seen_once(uint32_t *mask, uint32_t bit)
{
    if ((*mask & bit) != 0u) {
        return false;
    }
    *mask |= bit;
    return true;
}

/* The strict logging section: one key, two tokens, nothing else. */
/* A key is its exact decoded bytes: the parser's length plus a byte compare,
 * so an embedded NUL or any suffix is a different key, not a spelling. */
static bool
key_is(const struct json_string_s *name, const char *lit)
{
    size_t n = strlen(lit);
    return name != NULL && name->string_size == n &&
           memcmp(name->string, lit, n) == 0;
}

static moqr_result_t
parse_logging(struct json_object_s *o, moqr_cli_config_t *out, char *err,
              size_t err_len)
{
    bool seen_format = false;

    out->logging.format = MOQR_CLI_LOG_TEXT;
    for (struct json_object_element_s *e = o->start; e != NULL; e = e->next) {
        if (key_is(e->name, "format")) {
            struct json_string_s *s = json_value_as_string(e->value);
            if (seen_format) {
                cfg_err(err, err_len, "logging.format: duplicate key");
                return MOQR_ERR_INVAL;
            }
            seen_format = true;
            if (s == NULL) {
                cfg_err(err, err_len, "logging.format: need a string");
                return MOQR_ERR_INVAL;
            }
            if (s->string_size == 4u && memcmp(s->string, "text", 4u) == 0) {
                out->logging.format = MOQR_CLI_LOG_TEXT;
            } else if (s->string_size == 4u &&
                       memcmp(s->string, "json", 4u) == 0) {
                out->logging.format = MOQR_CLI_LOG_JSON;
            } else {
                cfg_err(err, err_len, "logging.format: expected \"text\" or \"json\"");
                return MOQR_ERR_INVAL;
            }
        } else {
            cfg_err(err, err_len, "logging: unknown key");
            return MOQR_ERR_INVAL;
        }
    }
    return MOQR_OK;
}

#ifdef MOQR_VERIFY_SEAM
moqr_result_t
moqr_cli_verify_refuse_json_logging(const moqr_cli_config_t *cfg, char *err,
                                    size_t err_len)
{
    if (cfg == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (cfg->logging.format == MOQR_CLI_LOG_JSON) {
        cfg_err(err, err_len, "logging.format: json is not available in the "
                              "verify and measure builds");
        return MOQR_ERR_INVAL;
    }
    return MOQR_OK;
}
#endif

static moqr_result_t
parse_admin(struct json_object_s *o, moqr_cli_config_t *out, char *err,
            size_t err_len)
{
    bool have_tcp = false;
    bool enabled_seen = false;
    bool enabled_val = true;
    uint32_t seen = 0;

    out->admin.enabled = false;
    out->admin.mode = MOQR_CLI_ADMIN_OFF;
    out->admin.port = 0;
    out->admin.host[0] = '\0';

    for (struct json_object_element_s *e = o->start; e != NULL; e = e->next) {
        const char *k = e->name->string;
        if (strcmp(k, "enabled") == 0) {
            if (!seen_once(&seen, 1u << 0)) {
                cfg_err(err, err_len, "admin.enabled: duplicate key");
                return MOQR_ERR_INVAL;
            }
            if (json_value_is_true(e->value)) {
                enabled_val = true;
            } else if (json_value_is_false(e->value)) {
                enabled_val = false;
            } else {
                cfg_err(err, err_len, "admin.enabled: need a boolean");
                return MOQR_ERR_INVAL;
            }
            enabled_seen = true;
        } else if (strcmp(k, "tcp") == 0) {
            struct json_object_s *t = json_value_as_object(e->value);
            bool have_port = false;
            uint32_t tseen = 0;
            if (!seen_once(&seen, 1u << 1)) {
                cfg_err(err, err_len, "admin.tcp: duplicate key");
                return MOQR_ERR_INVAL;
            }
            if (t == NULL) {
                cfg_err(err, err_len, "admin.tcp: need an object");
                return MOQR_ERR_INVAL;
            }
            have_tcp = true;
            snprintf(out->admin.host, sizeof(out->admin.host), "127.0.0.1");
            for (struct json_object_element_s *te = t->start; te != NULL;
                 te = te->next) {
                const char *tk = te->name->string;
                if (strcmp(tk, "host") == 0) {
                    if (!seen_once(&tseen, 1u << 0)) {
                        cfg_err(err, err_len, "admin.tcp.host: duplicate key");
                        return MOQR_ERR_INVAL;
                    }
                    if (!copy_string(te->value, out->admin.host,
                                     sizeof(out->admin.host))) {
                        cfg_err(err, err_len, "admin.tcp.host: invalid string");
                        return MOQR_ERR_INVAL;
                    }
                } else if (strcmp(tk, "port") == 0) {
                    uint64_t x = 0;
                    if (!seen_once(&tseen, 1u << 1)) {
                        cfg_err(err, err_len, "admin.tcp.port: duplicate key");
                        return MOQR_ERR_INVAL;
                    }
                    if (!num_u64(te->value, &x) || x == 0 || x > 65535) {
                        cfg_err(err, err_len, "admin.tcp.port: need 1..65535");
                        return MOQR_ERR_INVAL;
                    }
                    out->admin.port = (int)x;
                    have_port = true;
                } else {
                    cfg_err(err, err_len, "admin.tcp: unknown key");
                    return MOQR_ERR_INVAL;
                }
            }
            if (!have_port) {
                /* No default: a listener must never be opened by accident. */
                cfg_err(err, err_len, "admin.tcp.port is required");
                return MOQR_ERR_INVAL;
            }
            if (!admin_host_is_loopback(out->admin.host)) {
                cfg_err(err, err_len,
                        "admin.tcp.host: only loopback literals are accepted");
                return MOQR_ERR_INVAL;
            }
        } else {
            cfg_err(err, err_len, "admin: unknown key");
            return MOQR_ERR_INVAL;
        }
    }

    if (enabled_seen && !enabled_val) {
        if (have_tcp) {
            cfg_err(err, err_len,
                    "admin: enabled=false with an endpoint configured");
            return MOQR_ERR_INVAL;
        }
        out->admin.enabled = false;
        return MOQR_OK;
    }
    if (!have_tcp) {
        /* v1 has exactly one endpoint shape; "enabled" alone opens nothing. */
        cfg_err(err, err_len, "admin: tcp is required when enabled");
        return MOQR_ERR_INVAL;
    }
    out->admin.enabled = true;
    out->admin.mode = MOQR_CLI_ADMIN_TCP;
    return MOQR_OK;
}

/* The recognized keys of the webtransport object, as a closed set. Each may
 * appear at most once however the document spelled it, and a key that is not
 * exactly one of these is unknown -- a prefix, or a name carrying an embedded
 * NUL, matches nothing. */
enum {
    WT_KEY_HOST = 0,
    WT_KEY_PORT,
    WT_KEY_CERT,
    WT_KEY_KEY,
    WT_KEY_LANES,
    WT_KEY_PATH,
    WT_KEY_VERSIONS,
    WT_KEY_PROFILE,
    WT_KEY_ORIGIN_POLICY,
    WT_KEY_ALLOWED_ORIGINS,
    WT_KEY_COUNT
};

static const char *const k_wt_keys[WT_KEY_COUNT] = {
    "host", "port", "cert", "key", "lanes",
    "path", "versions", "profile", "origin_policy", "allowed_origins"
};

/* A message naming one recognized key. The key comes from the closed table by
 * index, so nothing the document wrote can reach the operator's screen. */
static void
cfg_err_wt_key(char *err, size_t err_len, int ki, const char *msg)
{
    if (err != NULL && err_len > 0 && ki >= 0 && ki < WT_KEY_COUNT) {
        snprintf(err, err_len, "webtransport.%s: %s", k_wt_keys[ki], msg);
    }
}

static int
wt_key_index(const struct json_string_s *name)
{
    for (int i = 0; i < WT_KEY_COUNT; i++) {
        if (key_is(name, k_wt_keys[i])) {
            return i;
        }
    }
    return -1;
}

static moqr_result_t
parse_webtransport(struct json_object_s *o, moqr_cli_config_t *out, char *err,
                   size_t err_len)
{
    out->wt.enabled = true;
    out->wt.lanes = 1;
    snprintf(out->wt.host, sizeof(out->wt.host), "0.0.0.0");
    snprintf(out->wt.path, sizeof(out->wt.path), "/moq");
    out->wt.profile = MOQR_CLI_WT_PROFILE_CURRENT;
    out->wt.origin_policy = MOQR_CLI_ORIGIN_POLICY_UNSET;
    out->wt.origin_count = 0;
    out->wt.origin_buf[0] = '\0';
    for (size_t i = 0; i < MOQR_CLI_MAX_ORIGINS; i++) {
        out->wt.origins[i] = NULL;
    }

    /* The messages below name these limits in words, so a changed limit must
     * change the text with it. */
    _Static_assert(MOQR_CLI_MAX_ORIGINS == 8, "the refusals name 8 entries");
    _Static_assert(MOQR_CLI_MAX_ORIGIN_BYTES == 320,
                   "the refusals name 320 bytes per entry");
    _Static_assert(MOQR_CLI_ORIGIN_COPY_BUDGET == 512,
                   "the refusals name a 512-byte total");

    bool have_versions = false;
    bool have_origins = false;
    bool seen[WT_KEY_COUNT] = { false };
    for (struct json_object_element_s *e = o->start; e != NULL; e = e->next) {
        int ki = wt_key_index(e->name);
        if (ki < 0) {
            cfg_err(err, err_len, "webtransport: unknown key");
            return MOQR_ERR_INVAL;
        }
        if (seen[ki]) {
            cfg_err_wt_key(err, err_len, ki, "duplicate key");
            return MOQR_ERR_INVAL;
        }
        seen[ki] = true;
        if (ki == WT_KEY_HOST) {
            if (!wt_copy(e->value, out->wt.host, sizeof(out->wt.host))) {
                cfg_err(err, err_len, "webtransport.host: invalid string");
                return MOQR_ERR_INVAL;
            }
        } else if (ki == WT_KEY_PORT) {
            uint64_t x = 0;
            if (!num_u64(e->value, &x) || x == 0 || x > 65535) {
                cfg_err(err, err_len, "webtransport.port: need 1..65535");
                return MOQR_ERR_INVAL;
            }
            out->wt.port = (int)x;
        } else if (ki == WT_KEY_CERT) {
            if (!wt_copy(e->value, out->wt.cert, sizeof(out->wt.cert))) {
                cfg_err(err, err_len, "webtransport.cert: invalid string");
                return MOQR_ERR_INVAL;
            }
        } else if (ki == WT_KEY_KEY) {
            if (!wt_copy(e->value, out->wt.key, sizeof(out->wt.key))) {
                cfg_err(err, err_len, "webtransport.key: invalid string");
                return MOQR_ERR_INVAL;
            }
        } else if (ki == WT_KEY_LANES) {
            uint64_t x = 0;
            if (!num_u64(e->value, &x) || x == 0 || x > MOQR_CLI_MAX_LANES) {
                cfg_err(err, err_len,
                        "webtransport.lanes: need 1..64");
                return MOQR_ERR_INVAL;
            }
            out->wt.lanes = (uint32_t)x;
        } else if (ki == WT_KEY_PATH) {
            if (!wt_copy(e->value, out->wt.path, sizeof(out->wt.path))) {
                cfg_err(err, err_len, "webtransport.path: invalid string");
                return MOQR_ERR_INVAL;
            }
            /* an origin-form request target, so the browser URL and the
             * listener agree on where the session is offered */
            if (out->wt.path[0] != '/') {
                cfg_err(err, err_len,
                        "webtransport.path: must begin with '/'");
                return MOQR_ERR_INVAL;
            }
        } else if (ki == WT_KEY_VERSIONS) {
            /* The same ordered draft set the raw listener offers, carried as
             * WebTransport subprotocols instead of ALPN. Order is preference. */
            struct json_array_s *arr = json_value_as_array(e->value);
            if (arr == NULL || arr->length == 0) {
                cfg_err(err, err_len,
                        "webtransport.versions: a non-empty array is required");
                return MOQR_ERR_INVAL;
            }
            if (arr->length > MOQR_CLI_MAX_VERSIONS) {
                cfg_err(err, err_len, "webtransport.versions: too many entries");
                return MOQR_ERR_INVAL;
            }
            size_t n = 0;
            for (struct json_array_element_s *ae = arr->start; ae != NULL;
                 ae = ae->next) {
                uint64_t v = 0;
                if (!num_u64(ae->value, &v) || (v != 16 && v != 18)) {
                    cfg_err(err, err_len,
                            "webtransport.versions: only 16 and 18 are known");
                    return MOQR_ERR_INVAL;
                }
                moq_version_t mv = (v == 16) ? MOQ_VERSION_DRAFT_16
                                             : MOQ_VERSION_DRAFT_18;
                for (size_t j = 0; j < n; j++) {
                    if (out->wt.versions[j] == mv) {
                        cfg_err(err, err_len,
                                "webtransport.versions: duplicate entry");
                        return MOQR_ERR_INVAL;
                    }
                }
                snprintf(out->wt.subproto_buf[n],
                         sizeof(out->wt.subproto_buf[n]),
                         "moqt-%" PRIu64, v);
                out->wt.subprotos[n] = out->wt.subproto_buf[n];
                out->wt.versions[n] = mv;
                n++;
            }
            out->wt.version_count = n;
            have_versions = true;
        } else if (ki == WT_KEY_PROFILE) {
            const char *p = NULL;
            size_t n = 0;
            if (!wt_str(e->value, &p, &n)) {
                cfg_err(err, err_len,
                        "webtransport.profile: need a string");
                return MOQR_ERR_INVAL;
            }
            if (wt_val_is(p, n, "current")) {
                out->wt.profile = MOQR_CLI_WT_PROFILE_CURRENT;
            } else if (wt_val_is(p, n, "d13_14_compat")) {
                out->wt.profile = MOQR_CLI_WT_PROFILE_D13_14_COMPAT;
            } else if (wt_val_is(p, n, "d02_rfc9297_compat")) {
                out->wt.profile = MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT;
            } else {
                cfg_err(err, err_len,
                        "webtransport.profile: only \"current\", "
                        "\"d13_14_compat\" and \"d02_rfc9297_compat\" "
                        "are known");
                return MOQR_ERR_INVAL;
            }
        } else if (ki == WT_KEY_ORIGIN_POLICY) {
            const char *p = NULL;
            size_t n = 0;
            if (!wt_str(e->value, &p, &n)) {
                cfg_err(err, err_len,
                        "webtransport.origin_policy: need a string");
                return MOQR_ERR_INVAL;
            }
            if (wt_val_is(p, n, "unset")) {
                out->wt.origin_policy = MOQR_CLI_ORIGIN_POLICY_UNSET;
            } else if (wt_val_is(p, n, "allow_any_non_opaque")) {
                out->wt.origin_policy =
                    MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE;
            } else if (wt_val_is(p, n, "allowlist")) {
                out->wt.origin_policy = MOQR_CLI_ORIGIN_POLICY_ALLOWLIST;
            } else if (wt_val_is(p, n, "allow_any_including_null")) {
                out->wt.origin_policy =
                    MOQR_CLI_ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL;
            } else {
                cfg_err(err, err_len,
                        "webtransport.origin_policy: only \"unset\", "
                        "\"allow_any_non_opaque\", \"allowlist\" and "
                        "\"allow_any_including_null\" are known");
                return MOQR_ERR_INVAL;
            }
        } else if (ki == WT_KEY_ALLOWED_ORIGINS) {
            /* Exact serialized-Origin bytes, stored as written. Nothing here
             * parses the Origin grammar or normalizes a value: entries are
             * compared byte for byte by the authorization decision, so a
             * helpful rewrite here would silently change who is admitted. */
            struct json_array_s *arr = json_value_as_array(e->value);
            if (arr == NULL) {
                cfg_err(err, err_len,
                        "webtransport.allowed_origins: need an array");
                return MOQR_ERR_INVAL;
            }
            have_origins = true;
            if (arr->length > MOQR_CLI_MAX_ORIGINS) {
                cfg_err(err, err_len,
                        "webtransport.allowed_origins: at most 8 entries");
                return MOQR_ERR_INVAL;
            }
            size_t used = 0;
            size_t cnt = 0;
            for (struct json_array_element_s *ae = arr->start; ae != NULL;
                 ae = ae->next) {
                const char *p = NULL;
                size_t n = 0;
                if (!wt_str(ae->value, &p, &n)) {
                    cfg_err_at(err, err_len,
                               "webtransport.allowed_origins: need a string "
                               "with no embedded NUL", cnt);
                    return MOQR_ERR_INVAL;
                }
                if (n == 0) {
                    cfg_err_at(err, err_len,
                               "webtransport.allowed_origins: must not be "
                               "empty", cnt);
                    return MOQR_ERR_INVAL;
                }
                if (n > MOQR_CLI_MAX_ORIGIN_BYTES) {
                    cfg_err_at(err, err_len,
                               "webtransport.allowed_origins: at most 320 "
                               "bytes", cnt);
                    return MOQR_ERR_INVAL;
                }
                for (size_t j = 0; j < cnt; j++) {
                    size_t pl = strlen(out->wt.origins[j]);
                    if (pl == n && memcmp(out->wt.origins[j], p, n) == 0) {
                        cfg_err_at(err, err_len,
                                   "webtransport.allowed_origins: duplicate "
                                   "entry", cnt);
                        return MOQR_ERR_INVAL;
                    }
                }
                if (n + 1 > sizeof(out->wt.origin_buf) - used) {
                    cfg_err(err, err_len,
                            "webtransport.allowed_origins: at most 512 bytes "
                            "in total including terminators");
                    return MOQR_ERR_INVAL;
                }
                memcpy(&out->wt.origin_buf[used], p, n);
                out->wt.origin_buf[used + n] = '\0';
                out->wt.origins[cnt] = &out->wt.origin_buf[used];
                used += n + 1;
                cnt++;
            }
            out->wt.origin_count = cnt;
        } else {
            cfg_err(err, err_len, "webtransport: unknown key");
            return MOQR_ERR_INVAL;
        }
    }
    /*
     * Cross-field rules, resolved only once every field of the object has been
     * collected, so the order the operator wrote the keys in cannot decide
     * whether the document is valid.
     */
    if (out->wt.origin_policy == MOQR_CLI_ORIGIN_POLICY_ALLOWLIST) {
        if (!have_origins || out->wt.origin_count == 0) {
            cfg_err(err, err_len,
                    "webtransport.allowed_origins: \"allowlist\" needs 1..8 "
                    "entries");
            return MOQR_ERR_INVAL;
        }
    } else if (have_origins) {
        /* Under every other policy -- an unset one included -- a list has no
         * meaning, so writing one is refused rather than quietly ignored. An
         * explicitly empty array is still a written list. */
        cfg_err(err, err_len,
                "webtransport.allowed_origins: only \"allowlist\" takes a "
                "list");
        return MOQR_ERR_INVAL;
    }
    if (out->wt.profile == MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT &&
        out->wt.origin_policy == MOQR_CLI_ORIGIN_POLICY_UNSET) {
        cfg_err(err, err_len,
                "webtransport.origin_policy: required when profile is "
                "\"d02_rfc9297_compat\"");
        return MOQR_ERR_INVAL;
    }
    if (!have_versions) {
        /* the facade default, stated here so the offered set is always
         * explicit in the resolved config rather than implied downstream */
        out->wt.versions[0] = MOQ_VERSION_DRAFT_18;
        out->wt.versions[1] = MOQ_VERSION_DRAFT_16;
        snprintf(out->wt.subproto_buf[0], sizeof(out->wt.subproto_buf[0]),
                 "moqt-18");
        snprintf(out->wt.subproto_buf[1], sizeof(out->wt.subproto_buf[1]),
                 "moqt-16");
        out->wt.subprotos[0] = out->wt.subproto_buf[0];
        out->wt.subprotos[1] = out->wt.subproto_buf[1];
        out->wt.version_count = 2;
    }
    /* The ordered set label, joined in preference order exactly as written. */
    out->wt.alpn_set[0] = '\0';
    for (size_t i = 0; i < out->wt.version_count; i++) {
        if (i > 0) {
            strncat(out->wt.alpn_set, "+",
                    sizeof(out->wt.alpn_set) - strlen(out->wt.alpn_set) - 1);
        }
        strncat(out->wt.alpn_set, out->wt.subproto_buf[i],
                sizeof(out->wt.alpn_set) - strlen(out->wt.alpn_set) - 1);
    }
    if (out->wt.port == 0) {
        cfg_err(err, err_len, "webtransport.port is required");
        return MOQR_ERR_INVAL;
    }
    if (out->wt.cert[0] == '\0' || out->wt.key[0] == '\0') {
        cfg_err(err, err_len,
                "webtransport requires its own cert and key");
        return MOQR_ERR_INVAL;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_cli_facade_caps(const moqr_cli_config_t *cfg, const moq_alloc_t *alloc,
                     uint32_t *out_raw_cap, uint32_t *out_wt_cap)
{
    if (cfg == NULL || out_raw_cap == NULL || out_wt_cap == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_shards_cfg_t scfg;
    moqr_cli_build_shards_cfg(cfg, alloc, &scfg);
    moqr_shards_limits_t slim;
    if (moqr_shards_cfg_resolve(&scfg, &slim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    /* Each facade admits against the shards it owns, from the same
     * usable-bindings-per-shard rule the combined ceiling is reported from. */
    uint64_t raw64 = (uint64_t)cfg->lanes * slim.usable_bindings;
    uint64_t wt64 = cfg->wt.enabled
                        ? (uint64_t)cfg->wt.lanes * slim.usable_bindings
                        : 0u;
    if (raw64 > UINT32_MAX || wt64 > UINT32_MAX ||
        raw64 + wt64 > UINT32_MAX) {
        return MOQR_ERR_INVAL;
    }
    *out_raw_cap = (uint32_t)raw64;
    *out_wt_cap = (uint32_t)wt64;
    return MOQR_OK;
}

uint32_t
moqr_cli_total_lanes(const moqr_cli_config_t *cfg)
{
    if (cfg == NULL) {
        return 0;
    }
    return cfg->lanes + (cfg->wt.enabled ? cfg->wt.lanes : 0);
}

bool
moqr_cli_config_has_webtransport(const moqr_cli_config_t *cfg)
{
    return cfg != NULL && cfg->wt.enabled;
}

moqr_result_t
moqr_cli_shard_plan(const moqr_cli_config_t *cfg, moqr_cli_shard_plan_t *out,
                    char *err, size_t err_len)
{
    if (cfg == NULL || out == NULL) {
        cfg_err(err, err_len, "shard plan: null argument");
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg->lanes == 0) {
        cfg_err(err, err_len, "listener.lanes must be at least 1");
        return MOQR_ERR_INVAL;
    }
    out->raw_first = 0;
    out->raw_count = cfg->lanes;
    out->wt_first = cfg->lanes;
    out->wt_count = cfg->wt.enabled ? cfg->wt.lanes : 0;
    if (cfg->wt.enabled && cfg->wt.lanes == 0) {
        cfg_err(err, err_len, "webtransport.lanes must be at least 1");
        return MOQR_ERR_INVAL;
    }
    /* The runtime allocates one shard per lane across BOTH listeners, so the
     * combined count is what has to fit -- refused here, before any shard,
     * facade or listener exists. */
    if (out->raw_count > MOQR_CLI_MAX_LANES - out->wt_count) {
        cfg_err(err, err_len,
                "listener.lanes + webtransport.lanes must not exceed 64");
        return MOQR_ERR_INVAL;
    }
    out->total_shards = out->raw_count + out->wt_count;
    return MOQR_OK;
}

uint32_t
moqr_cli_shard_of_raw_lane(const moqr_cli_shard_plan_t *p, uint32_t lane)
{
    if (p == NULL || lane >= p->raw_count) {
        return UINT32_MAX;
    }
    return p->raw_first + lane;
}

uint32_t
moqr_cli_shard_of_wt_lane(const moqr_cli_shard_plan_t *p, uint32_t lane)
{
    if (p == NULL || lane >= p->wt_count) {
        return UINT32_MAX;
    }
    return p->wt_first + lane;
}

moqr_result_t
moqr_cli_config_parse(const char *json, size_t len, moqr_cli_config_t *out,
                      char *err, size_t err_len)
{
    if (json == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    moqr_core_relay_cfg_init_sized(&out->core, sizeof(out->core), NULL);
    snprintf(out->host, sizeof(out->host), "0.0.0.0");
    out->lanes = 1;   /* default single-lane; listener.lanes overrides */
    cfg_err(err, err_len, "ok");

    struct json_value_s *root = json_parse(json, len);
    if (root == NULL) {
        cfg_err(err, err_len, "not valid JSON");
        return MOQR_ERR_INVAL;
    }
    struct json_object_s *o = json_value_as_object(root);
    moqr_result_t rc = MOQR_OK;
    bool have_listener = false;
    bool seen_admin = false;
    bool seen_logging = false;
    bool seen_wt = false;
    out->logging.format = MOQR_CLI_LOG_TEXT;
    if (o == NULL) {
        cfg_err(err, err_len, "top level must be an object");
        rc = MOQR_ERR_INVAL;
    }
    for (struct json_object_element_s *e = o != NULL ? o->start : NULL;
         e != NULL && rc == MOQR_OK; e = e->next) {
        const char *k = e->name->string;
        if (strcmp(k, "listener") == 0) {
            struct json_object_s *lo = json_value_as_object(e->value);
            if (lo == NULL) {
                cfg_err(err, err_len, "listener: need an object");
                rc = MOQR_ERR_INVAL;
            } else {
                rc = parse_listener(lo, out, err, err_len);
                have_listener = rc == MOQR_OK;
            }
        } else if (strcmp(k, "admin") == 0) {
            struct json_object_s *ao = json_value_as_object(e->value);
            if (seen_admin) {
                cfg_err(err, err_len, "admin: duplicate key");
                rc = MOQR_ERR_INVAL;
                break;
            }
            seen_admin = true;
            if (ao == NULL) {
                cfg_err(err, err_len, "admin: need an object");
                rc = MOQR_ERR_INVAL;
            } else {
                rc = parse_admin(ao, out, err, err_len);
            }
        } else if (key_is(e->name, "logging")) {
            struct json_object_s *lg = json_value_as_object(e->value);
            if (seen_logging) {
                cfg_err(err, err_len, "logging: duplicate key");
                rc = MOQR_ERR_INVAL;
                break;
            }
            seen_logging = true;
            if (lg == NULL) {
                cfg_err(err, err_len, "logging: need an object");
                rc = MOQR_ERR_INVAL;
            } else {
                rc = parse_logging(lg, out, err, err_len);
            }
        } else if (key_is(e->name, "webtransport")) {
            struct json_object_s *wo = json_value_as_object(e->value);
            if (seen_wt) {
                cfg_err(err, err_len, "webtransport: duplicate key");
                rc = MOQR_ERR_INVAL;
                break;
            }
            seen_wt = true;
            if (wo == NULL) {
                cfg_err(err, err_len, "webtransport: need an object");
                rc = MOQR_ERR_INVAL;
            } else {
                rc = parse_webtransport(wo, out, err, err_len);
            }
        } else if (strcmp(k, "budgets") == 0) {
            struct json_object_s *bo = json_value_as_object(e->value);
            if (bo == NULL) {
                cfg_err(err, err_len, "budgets: need an object");
                rc = MOQR_ERR_INVAL;
            } else {
                rc = parse_budgets(bo, out, err, err_len);
            }
        } else if (strcmp(k, "telemetry") == 0) {
            struct json_object_s *to = json_value_as_object(e->value);
            if (to == NULL) {
                cfg_err(err, err_len, "telemetry: need an object");
                rc = MOQR_ERR_INVAL;
            } else {
                rc = parse_telemetry(to, out, err, err_len);
            }
        } else if (strcmp(k, "auth") == 0) {
            struct json_object_s *ao = json_value_as_object(e->value);
            if (ao == NULL) {
                cfg_err(err, err_len, "auth: need an object");
                rc = MOQR_ERR_INVAL;
            } else {
                rc = parse_auth(ao, out, err, err_len);
            }
        } else if (strcmp(k, "linger_us") == 0) {
            uint64_t x = 0;
            if (!num_u64(e->value, &x)) {
                cfg_err(err, err_len, "linger_us: need a number");
                rc = MOQR_ERR_INVAL;
            } else {
                out->core.linger_us = x;
            }
        } else {
            cfg_err(err, err_len, "unknown top-level key");
            rc = MOQR_ERR_INVAL;
        }
    }
    if (rc == MOQR_OK && !have_listener) {
        cfg_err(err, err_len, "listener is required");
        rc = MOQR_ERR_INVAL;
    }
    if (rc == MOQR_OK && out->alpn_count == 0) {
        /* Default: exactly the newest draft, a single offer. */
        snprintf(out->alpn_buf[0], sizeof(out->alpn_buf[0]), "moqt-18");
        out->alpns[0] = out->alpn_buf[0];
        out->alpn_count = 1;
        out->version = 18;
        out->versions[0] = MOQ_VERSION_DRAFT_18;
        out->version_count = 1;
    }
    if (rc == MOQR_OK) {
        /* One deterministic label for the whole offered set: the single ALPN
         * when there is one -- preserving exact-listener output byte for byte
         * -- else the ordered ALPNs joined by '+'. */
        size_t o = 0;
        out->alpn_set[0] = '\0';
        for (size_t i = 0; i < out->alpn_count; i++) {
            int w = snprintf(out->alpn_set + o, sizeof(out->alpn_set) - o,
                             "%s%s", i == 0 ? "" : "+", out->alpns[i]);
            if (w < 0 || (size_t)w >= sizeof(out->alpn_set) - o) {
                cfg_err(err, err_len, "listener.versions: label overflow");
                free(root);
                return MOQR_ERR_INVAL;
            }
            o += (size_t)w;
        }
    }
    free(root);
    return rc;
}

#ifdef MOQR_VERIFY_SEAM
/* Blocked-scenario constraint seam (see config.h). Values cached by the ONE
 * load call so every later consumer (describe + compose, any lane) sees the
 * same constraint; 0 = unset = library default. */
static uint32_t g_verify_bind_max_sgs;
static uint32_t g_verify_session_max_sgs;

/* Strict decimal for the seam: 1..65535, no leading zeros, digits only.
 * (The canonical-value rule the blocked bench's --seconds parser pinned:
 * "08" and "8junk" are refusals, not values.) */
static bool
verify_env_u32(const char *name, uint32_t *out, char *err, size_t err_len)
{
    *out = 0;
    const char *v = getenv(name);
    if (v == NULL) {
        return true;   /* unset = default */
    }
    if (*v < '1' || *v > '9') {
        snprintf(err, err_len, "%s: need decimal 1..65535 (no leading "
                               "zeros), got \"%s\"", name, v);
        return false;
    }
    uint32_t acc = 0;
    for (const char *c = v; *c != '\0'; c++) {
        if (*c < '0' || *c > '9' || acc > 6553u ||
            (acc == 6553u && *c > '5')) {
            snprintf(err, err_len, "%s: need decimal 1..65535 (no leading "
                                   "zeros), got \"%s\"", name, v);
            return false;
        }
        acc = acc * 10u + (uint32_t)(*c - '0');
    }
    *out = acc;
    return true;
}

moqr_result_t
moqr_cli_verify_env_load(char *err, size_t err_len)
{
    if (!verify_env_u32("MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS",
                        &g_verify_bind_max_sgs, err, err_len) ||
        !verify_env_u32("MOQR_VERIFY_SESSION_MAX_OPEN_SUBGROUPS",
                        &g_verify_session_max_sgs, err, err_len)) {
        g_verify_bind_max_sgs = 0;
        g_verify_session_max_sgs = 0;
        return MOQR_ERR_INVAL;
    }
    return MOQR_OK;
}

uint32_t
moqr_cli_verify_bind_max_sgs(void)
{
    return g_verify_bind_max_sgs;
}

uint32_t
moqr_cli_verify_session_max_sgs(void)
{
    return g_verify_session_max_sgs;
}
#endif /* MOQR_VERIFY_SEAM */

void
moqr_cli_build_shards_cfg(const moqr_cli_config_t *cfg,
                          const moq_alloc_t *alloc, moqr_shards_cfg_t *out)
{
    moqr_shards_cfg_init_sized(out, sizeof(*out), alloc);
    out->shards = (uint16_t)moqr_cli_total_lanes(cfg);
    out->trace_ring_records = cfg->telemetry.trace_ring_records;
    out->live_visibility = true;
    moqr_cli_core_cfg_from(cfg, alloc, &out->core_cfg);
    /* budgets.cross_shard, zero = library default (the shared shard
     * resolver stays authoritative for defaults and cross-field rules). */
    out->journal_entries = cfg->cross_shard.journal_entries;
    out->mailbox_entries = cfg->cross_shard.mailbox_entries;
    out->demand_channel_entries = cfg->cross_shard.demand_channel_entries;
    out->demand_channel_bytes = cfg->cross_shard.demand_channel_bytes;
    out->pending_demand_entries = cfg->cross_shard.pending_demands;
    out->pump_subgroup_slots = cfg->cross_shard.subgroup_slots;
    /* Strict per-lane clamp: at lanes > 1 every shard's manager consumes
     * `lanes` core binding slots, so each lane's binding admits only the
     * external remainder — the shard boundary then fails closed exactly
     * where the model says it will. */
    const uint32_t total_lanes = moqr_cli_total_lanes(cfg);
    if (total_lanes > 1) {
        moqr_core_limits_t clim;
        if (moqr_core_limits_resolve(&out->core_cfg, &clim) == MOQR_OK &&
            clim.max_bindings > total_lanes) {
            out->bind_cfg.max_conns = clim.max_bindings - total_lanes;
        }
    }
    /* Production admission is automatically ON exactly when the relay runs
     * multiple lanes: a single-lane relay is the direct M1 path with no
     * cross-shard plane to admit into (and no manager at K == 1), while a
     * multi-lane relay must forward remote-owned demand across the shard
     * boundary for a subscriber to reach a publisher on another lane. This
     * is the ONE place the rule lives — capacity and serve both consume
     * this output. There is no user-facing admission key or toggle. */
    /* Across BOTH listeners: a WebTransport subscriber reaching a raw
     * publisher crosses shards exactly as a cross-lane subscriber does, so a
     * one-lane-each dual config still needs owner-side admission. */
    out->admit_remote_demand = moqr_cli_total_lanes(cfg) > 1;
#ifdef MOQR_VERIFY_SEAM
    /* Blocked-scenario seam: constrain every shard's bind subgroup slot
     * pool. Applied in THIS builder so describe and serve stay one config —
     * the printed ceiling is the constrained pool, never the default. */
    if (moqr_cli_verify_bind_max_sgs() != 0) {
        out->bind_cfg.max_open_subgroups = moqr_cli_verify_bind_max_sgs();
    }
#endif
}

moqr_result_t
moqr_cli_serve_compose(const moqr_cli_config_t *cfg, const moq_alloc_t *alloc,
                       moqr_shards_cfg_t *out_scfg,
                       uint32_t *out_max_connections)
{
    if (cfg == NULL || out_scfg == NULL || out_max_connections == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_shards_cfg_t scfg;
    moqr_cli_build_shards_cfg(cfg, alloc, &scfg);
    moqr_shards_limits_t slim;
    if (moqr_shards_cfg_resolve(&scfg, &slim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    /* Facade admission cap from the SAME rule the capacity model reports:
     * usable external bindings per shard, times lanes (checked). */
    uint64_t cap64 =
        (uint64_t)moqr_cli_total_lanes(cfg) * slim.usable_bindings;
    *out_max_connections =
        cap64 > UINT32_MAX ? UINT32_MAX : (uint32_t)cap64;
    *out_scfg = scfg;
    return MOQR_OK;
}

moqr_result_t
moqr_cli_config_validate(const moqr_cli_config_t *cfg,
                         const moq_alloc_t *alloc)
{
    if (cfg == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_shards_cfg_t scfg;
    moqr_cli_build_shards_cfg(cfg, alloc, &scfg);
    moqr_shards_limits_t lim;
    return moqr_shards_cfg_resolve(&scfg, &lim) == MOQR_OK ? MOQR_OK
                                                           : MOQR_ERR_INVAL;
}

/* The admin endpoint's allocator-owned footprint.
 *
 * EXACT, and derived from the listener's own checked descriptor rather than
 * from a second formula here. The descriptor is a size-only translation unit,
 * so a config consumer learns the ceiling without linking socket or thread
 * code. A refusal to bound the document is reported as UINT64_MAX -- a wrapped
 * ceiling must never be presented as a smaller one.
 *
 * The thread stack is a RESERVATION, reported separately and never folded into
 * the allocator-request total. Kernel socket buffers stay in the documented
 * exclusions for the same reason. */
static void
admin_capacity(const moqr_cli_config_t *cfg, uint32_t lanes, uint64_t *out_bytes,
               uint64_t *out_stack)
{
    moqr_admin_listen_footprint_t fp;

    *out_bytes = 0;
    *out_stack = 0;
    if (cfg == NULL || !cfg->admin.enabled) {
        return;
    }
    if (moqr_admin_listen_footprint(lanes, &fp) != MOQR_OK) {
        *out_bytes = UINT64_MAX;
        *out_stack = MOQR_CLI_ADMIN_STACK_BYTES;
        return;
    }
    /* The owner context's own storage: one copied snapshot row and one render
     * view per lane, held for the endpoint's lifetime so a scrape performs no
     * allocation. It belongs to the coordinator rather than to the listener
     * object, so it is a separate named term rather than a hidden one. */
    *out_bytes = moqr_cap_add(
        fp.total_alloc_bytes,
        moqr_cap_mul((uint64_t)lanes,
                     moqr_cap_add(
                         (uint64_t)sizeof(moqr_cli_snapshot_stats_t),
                         (uint64_t)sizeof(moqr_snapshot_view_t))));
    /* ...and the coordinator's signal-sink body: the frozen Prometheus
     * projection, one document wide, held so a signal dump performs no
     * allocation and reads no live lane. Sized by the same checked bound the
     * listener's Prometheus bank uses. */
    *out_bytes = moqr_cap_add(*out_bytes,
                              fp.body_cap[MOQR_OBS_FMT_PROMETHEUS_004]);
    /* ...and the coordinator's immutable /api/v1/info document, one bound
     * wide plus its terminator, rendered once before activation. */
    *out_bytes = moqr_cap_add(*out_bytes,
                              moqr_cap_add(moqr_cli_info_bound(), 1u));
    *out_stack = fp.thread_stack_bytes;
}

moqr_result_t
moqr_cli_describe_capacity(const moqr_cli_config_t *cfg,
                           const moq_alloc_t *alloc, size_t serve_ctx_bytes,
                           moqr_cli_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (moqr_cli_total_lanes(cfg) <= 1) {
        /* Cross-field validation FIRST, through the same shared resolver
         * lanes>1 uses: "lanes=1 accepts the object" means valid inert
         * settings — an explicit demand_channel_bytes below one resolved
         * log record is invalid at EVERY lane count, never bypassed. */
        if (moqr_cli_config_validate(cfg, alloc) != MOQR_OK) {
            return MOQR_ERR_INVAL;
        }
        /* The direct lanes=1 composition: core + binding + trace — exactly
         * what cmd_serve allocates (no shard container, no CLI rows). */
        moqr_core_relay_cfg_t core = cfg->core;
        core.alloc = alloc;
        moqr_core_capacity_t cap;
        if (moqr_core_capacity_describe(&core, &cap) != MOQR_OK) {
            return MOQR_ERR_INVAL;
        }
        moqr_core_limits_t clim;
        if (moqr_core_limits_resolve(&core, &clim) != MOQR_OK) {
            return MOQR_ERR_INVAL;
        }
        moqr_bind_cfg_t bind_tmpl;
        moqr_bind_cfg_init_sized(&bind_tmpl, sizeof(bind_tmpl), alloc);
        moqr_bind_capacity_t bc;
        if (moqr_bind_capacity_describe(&bind_tmpl, &clim, &bc) != MOQR_OK) {
            return MOQR_ERR_INVAL;
        }
        moqr_bind_limits_t blim;
        (void)moqr_bind_cfg_resolve(&bind_tmpl, &clim, &blim);
        out->core_structure_bytes = cap.structure_bytes;
        out->core_payload_bytes = cap.payload_bytes;
        out->bind_structure_bytes = bc.total_bytes;
        out->trace_bytes =
            moqr_trace_bytes(cfg->telemetry.trace_ring_records);
        out->usable_bindings_per_shard = blim.max_conns < clim.max_bindings
                                             ? blim.max_conns
                                             : clim.max_bindings;
        /* One permanent snapshot row: cmd_serve holds it for the life of the
         * serve so K=1 and K>1 share one collect/render path. Counted like
         * every other permanent request; stack objects and the renderer's
         * transient buffers stay outside the ceiling, as the model states. */
        out->cli_runtime_bytes = moqr_cli_snapshot_bytes(1u);
        /* K=1 uses the same publication path, so the admin endpoint costs the
         * same here as it does above one lane. */
        admin_capacity(cfg, 1u, &out->admin_bytes,
                       &out->admin_thread_stack_bytes);
        out->cli_runtime_bytes =
            moqr_cap_add(out->cli_runtime_bytes, out->admin_bytes);
        out->total_bytes = moqr_cap_add(
            moqr_cap_add(moqr_cap_add(out->core_structure_bytes,
                                      out->core_payload_bytes),
                         moqr_cap_add(out->bind_structure_bytes,
                                      out->trace_bytes)),
            out->cli_runtime_bytes);
        if (out->total_bytes == UINT64_MAX) {
            memset(out, 0, sizeof(*out));
            return MOQR_ERR_INVAL;
        }
        return MOQR_OK;
    }
    /* lanes > 1: the SAME builder serve consumes, described whole, plus the
     * CLI's own multi-lane context and snapshot rows. */
    moqr_shards_cfg_t scfg;
    moqr_cli_build_shards_cfg(cfg, alloc, &scfg);
    moqr_shards_capacity_t sc;
    moqr_result_t rc = moqr_shards_capacity_describe(&scfg, &sc);
    if (rc != MOQR_OK) {
        return rc;
    }
    out->core_structure_bytes = sc.core_structure_bytes;
    out->core_payload_bytes = sc.core_payload_bytes;
    out->bind_structure_bytes = sc.bind_structure_bytes;
    out->trace_bytes = sc.trace_bytes;
    out->cross_shard_bytes = moqr_cap_add(
        moqr_cap_add(sc.shards_structure_bytes, sc.channel_byte_ceiling),
        moqr_cap_add(sc.canon_byte_ceiling, sc.staging_byte_ceiling));
    out->cli_runtime_bytes = moqr_cap_add(
        serve_ctx_bytes, moqr_cli_snapshot_bytes(moqr_cli_total_lanes(cfg)));
    admin_capacity(cfg, moqr_cli_total_lanes(cfg), &out->admin_bytes,
                   &out->admin_thread_stack_bytes);
    out->cli_runtime_bytes =
        moqr_cap_add(out->cli_runtime_bytes, out->admin_bytes);
    out->usable_bindings_per_shard = sc.usable_bindings_per_shard;
    out->total_bytes =
        moqr_cap_add(sc.relay_alloc_ceiling, out->cli_runtime_bytes);
    if (out->total_bytes == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* wrapped: refuse, never under-report */
    }
    return MOQR_OK;
}

void
moqr_cli_core_cfg_from(const moqr_cli_config_t *cfg, const moq_alloc_t *alloc,
                       moqr_core_relay_cfg_t *out)
{
    *out = cfg->core;
    out->alloc = alloc;
    /* Install the toy authorizer when configured; allow-all leaves the hook
     * NULL. The ctx borrows cfg->auth (which must outlive the core, and is
     * read-only — safe to share across shard threads); the uintptr_t hop
     * passes the const policy through the void* ctx cleanly. */
    if (cfg->auth.mode == MOQR_CLI_AUTH_TOY) {
        out->authorize = moqr_auth_toy_authorize;
        out->authorize_ctx = (void *)(uintptr_t)&cfg->auth.toy;
    }
}

moqr_result_t
moqr_cli_build_core(const moqr_cli_config_t *cfg, const moq_alloc_t *alloc,
                    moqr_trace_t **trace_out, moqr_core_t **core_out)
{
    if (cfg == NULL || alloc == NULL || trace_out == NULL ||
        core_out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *trace_out = NULL;
    *core_out = NULL;

    moqr_trace_t *trace = NULL;
    moqr_result_t rc =
        moqr_trace_create(alloc, cfg->telemetry.trace_ring_records, &trace);
    if (rc != MOQR_OK) {
        return rc;
    }

    moqr_core_relay_cfg_t core_cfg;
    moqr_cli_core_cfg_from(cfg, alloc, &core_cfg);
    core_cfg.trace = trace;
    moqr_core_t *core = NULL;
    rc = moqr_core_create(&core_cfg, &core);
    if (rc != MOQR_OK) {
        moqr_trace_destroy(trace);
        return rc;
    }

    *trace_out = trace;
    *core_out = core;
    return MOQR_OK;
}

moqr_result_t
moqr_cli_config_load(const char *path, moqr_cli_config_t *out, char *err,
                     size_t err_len)
{
    if (path == NULL) {
        return MOQR_ERR_INVAL;
    }
    FILE *f = fopen(path, "rb");
    if (f == NULL) {
        cfg_err(err, err_len, "cannot open config file");
        return MOQR_ERR_INVAL;
    }
    char buf[64 * 1024];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    bool truncated = fgetc(f) != EOF;
    fclose(f);
    if (truncated) {
        cfg_err(err, err_len, "config file too large (64 KiB max)");
        return MOQR_ERR_INVAL;
    }
    buf[n] = '\0';
    return moqr_cli_config_parse(buf, n, out, err, err_len);
}

/* The single production decision point for config -> transport versions. Both
 * serve compositions route through it, so they cannot drift apart. Transport
 * neutral on purpose: this translation unit is also linked into ungated test
 * binaries that must not depend on an adapter header. */
void
moqr_cli_version_plan(const moqr_cli_config_t *cfg,
                      moqr_cli_version_plan_t *out)
{
    out->exact = 0;
    out->list = NULL;
    out->count = 0;
    if (cfg == NULL || cfg->version_count == 0) {
        return;
    }
    if (cfg->version_count == 1) {
        /* The exact plan: a non-zero version and NO list, which is how the
         * managed adapter spells "exact-version listener". count stays 0. */
        out->exact = cfg->versions[0];
        return;
    }
    out->list = cfg->versions;
    out->count = cfg->version_count;
}
