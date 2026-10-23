/*
 * browser_receive -- the service-tier media receiver running in a web page.
 *
 * Build (Emscripten; see examples/browser/CMakeLists.txt):
 *   emcmake cmake -S . -B build/wasm -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON \
 *       -DMOQ_BUILD_MEDIA_OBJECT=ON -DMOQ_BUILD_WASM_WT_MANAGED=ON
 *   cmake --build build/wasm --target moq_example_browser_receive
 * Serve build/wasm/examples/browser/ over http://localhost (a secure context)
 * and open index.html?url=https://relay.example:4433/moq&ns=live/cam1
 *
 * The page uses the same public calls as examples/service/media_receive.c:
 * moq_endpoint_connect and moq_media_receiver_attach. What differs is the
 * thread: a page has one, and cannot block, so connect() returns before the
 * connection is up, the endpoint's backend is the browser's WebTransport API
 * (MOQ_TRANSPORT_BACKEND_BROWSER; AUTO resolves to it), and the page polls the
 * receiver from a timer instead of calling moq_media_receiver_wait().
 * The relay must serve WebTransport with a certificate the browser trusts,
 * and name a MoQT subprotocol (moqt-22 or moqt-18).
 */
#include <emscripten.h>
#include <moq/endpoint.h>
#include <moq/media_receiver.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Page-side logger: Module.log(string). */
EM_JS(void, page_log, (const char *s), { Module.log(UTF8ToString(s)); });

static void say(const char *fmt, ...)
{
    char b[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(b, sizeof(b), fmt, ap);
    va_end(ap);
    page_log(b);
}

static moq_endpoint_t *g_ep;
static moq_media_receiver_t *g_rx;
static unsigned long g_objects;
static unsigned long g_media_bytes;
static int g_catalog;

/* Connects and attaches a receiver; `draft` 0 offers 22 then 18, else pins it. */
EMSCRIPTEN_KEEPALIVE int receive_start(const char *url, const char *ns_slash, int draft)
{
    static char nsbuf[256];
    static moq_bytes_t parts[16];
    snprintf(nsbuf, sizeof(nsbuf), "%s", ns_slash);
    size_t n = 0;
    for (char *p = strtok(nsbuf, "/"); p && n < 16; p = strtok(NULL, "/")) {
        parts[n].data = (const uint8_t *)p;
        parts[n].len = strlen(p);
        n++;
    }

    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init_sized(&ec, sizeof(ec));
    ec.url.data = (const uint8_t *)url;
    ec.url.len = strlen(url);
    static const moq_version_t k_offer[] = { MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18 };
    static moq_version_t pinned;
    ec.versions.struct_size = sizeof(moq_version_offer_t);
    if (draft) {
        pinned = (moq_version_t)draft;
        ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
        ec.versions.versions = &pinned;
        ec.versions.version_count = 1;
    } else {
        ec.versions.policy = MOQ_VERSION_POLICY_LIST;
        ec.versions.versions = k_offer;
        ec.versions.version_count = 2;
    }
    moq_result_t rc = moq_endpoint_connect(&ec, &g_ep);
    if (rc != MOQ_OK) return (int)rc;

    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.endpoint = NULL;                 /* attach mode: we own g_ep */
    rcfg.namespace_.parts = parts;
    rcfg.namespace_.count = n;
    rcfg.auto_subscribe = true;
    return (int)moq_media_receiver_attach(g_ep, &rcfg, &g_rx);
}

/* Drains track events and media objects; call it from a timer. */
EMSCRIPTEN_KEEPALIVE void receive_poll(void)
{
    if (!g_rx) return;
    moq_media_track_event_t tev;
    while (moq_media_receiver_poll_track(g_rx, &tev, sizeof(tev)) == MOQ_OK) {
        if (tev.kind == MOQ_MEDIA_TRACK_ADDED && tev.desc) {
            say("track %.*s %.*s", (int)tev.desc->name.len, (const char *)tev.desc->name.data,
                (int)tev.desc->codec.len, (const char *)tev.desc->codec.data);
        } else if (tev.kind == MOQ_MEDIA_CATALOG_READY) {
            g_catalog = 1;
            say("catalog ready");
        }
    }
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(g_rx, &obj, sizeof(obj)) == MOQ_OK) {
        size_t len = obj.packaging == MOQ_MEDIA_PACKAGING_CMAF ? obj.mdat_len : obj.payload.len;
        if (g_objects == 0)
            say("first object: %s, pts %llu us, %zu bytes", obj.keyframe ? "key" : "delta",
                (unsigned long long)obj.presentation_time_us, len);
        g_objects++;
        g_media_bytes += len;
        moq_media_object_cleanup(&obj);
    }
}

EMSCRIPTEN_KEEPALIVE unsigned long receive_objects(void) { return g_objects; }
EMSCRIPTEN_KEEPALIVE unsigned long receive_bytes(void) { return g_media_bytes; }
EMSCRIPTEN_KEEPALIVE int receive_catalog_ready(void) { return g_catalog; }
EMSCRIPTEN_KEEPALIVE int receive_version(void) { return g_ep ? (int)moq_endpoint_negotiated_version(g_ep) : 0; }

EMSCRIPTEN_KEEPALIVE void receive_stop(void)
{
    if (g_rx) moq_media_receiver_destroy(g_rx);
    if (g_ep) {
        moq_endpoint_stop(g_ep);
        moq_endpoint_destroy(g_ep);
    }
    g_rx = NULL;
    g_ep = NULL;
}
