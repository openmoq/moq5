// Browser side of moq_wasm_wt_managed (see wasm_wt_managed.c): carries a libmoq
// session over the browser's WebTransport API. Linked with --js-library.
//
// Every libmoq callback below runs on the page's one thread. The C side owns
// stream ids' meaning only through the transport bridge; the ids here are
// opaque keys, unique for the connection and never reused.
mergeInto(LibraryManager.library, {
  moq_wasm_wt_js_supported: function () {
    return (typeof WebTransport !== 'undefined') ? 1 : 0;
  },

  moq_wasm_wt_js_connect: function (m, urlPtr, protoPtr) {
    var M = Module;
    var url = UTF8ToString(urlPtr);
    var protocols = UTF8ToString(protoPtr).split(',')
      .map(function (s) { return s.trim().replace(/^"|"$/g, ''); })
      .filter(function (s) { return s.length > 0; });
    M.moqWasmWt = M.moqWasmWt || { conns: {} };
    var c = { wt: null, nextId: 1, streams: {}, timer: null, timerAt: 0, dead: false, dgw: null };
    M.moqWasmWt.conns[m] = c;

    function cstr(s) {
      var b = new TextEncoder().encode(s);
      var p = M._moq_wasm_wt_alloc(b.length + 1);
      HEAPU8.set(b, p);
      HEAPU8[p + b.length] = 0;
      return p;
    }
    function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }
    function errCode(e) { return (e && typeof e.streamErrorCode === 'number') ? e.streamErrorCode : 0; }

    // Hands a chunk of a peer stream to libmoq, honouring its backpressure: a
    // WOULD_BLOCK (-8) took the bytes but wants a pause, an INPUT_NOT_CONSUMED
    // (-15) took nothing and wants the same chunk again; both wait until the
    // stream has no pending work.
    async function deliver(id, kind, u8, fin) {
      var p = M._moq_wasm_wt_alloc(u8.length);
      HEAPU8.set(u8, p);
      try {
        for (;;) {
          var rc = M._moq_wasm_wt_on_stream_bytes(m, id, kind, p, u8.length, fin ? 1 : 0);
          // Anything but success or the two flow-control answers is a fatal result for the session: say so.
          if (rc < 0 && rc !== -8 && rc !== -15) console.warn('[moq wasm_wt] stream ' + id + ' (' + (kind ? 'bidi' : 'uni') + ') bytes=' + u8.length + ' fin=' + fin + ' -> result ' + rc);
          if (rc !== -8 && rc !== -15) return;
          do {
            await sleep(2);
            if (c.dead) return;
          } while (M._moq_wasm_wt_stream_pending(m, id));
          if (rc === -8) return;       // the bytes were taken; only the pause was asked for
        }
      } finally { M._moq_wasm_wt_free(p); }
    }

    async function readLoop(id, readable, kind) {
      var r = readable.getReader();
      c.streams[id].r = r;
      try {
        for (;;) {
          var res = await r.read();
          if (c.dead) return;
          if (res.done) { await deliver(id, kind, new Uint8Array(0), true); return; }
          await deliver(id, kind, res.value, false);
        }
      } catch (e) {
        if (!c.dead) M._moq_wasm_wt_on_stream_reset(m, id, errCode(e));
      }
    }

    async function acceptLoop(readable, kind) {
      var r = readable.getReader();
      try {
        for (;;) {
          var res = await r.read();
          if (res.done || c.dead) return;
          var id = c.nextId++;
          var e = { w: null, r: null, chain: Promise.resolve() };
          c.streams[id] = e;
          if (kind) { e.w = res.value.writable.getWriter(); readLoop(id, res.value.readable, 1); }
          else readLoop(id, res.value, 0);
        }
      } catch (e) { /* the connection ended; closed handles it */ }
    }

    async function datagramLoop() {
      var r = c.wt.datagrams.readable.getReader();
      try {
        for (;;) {
          var res = await r.read();
          if (res.done || c.dead) return;
          var p = M._moq_wasm_wt_alloc(res.value.length);
          HEAPU8.set(res.value, p);
          M._moq_wasm_wt_on_datagram(m, p, res.value.length);
          M._moq_wasm_wt_free(p);
        }
      } catch (e) { /* ended with the connection */ }
    }

    try {
      var options = protocols.length ? { protocols: protocols } : {};
      // A relay with a self-signed certificate (local testing) is trusted by pinning its hash: the page sets
      // Module.moqWasmWtCertHashes = [{algorithm: 'sha-256', value: <ArrayBuffer>}] before opening a connection. The
      // browser accepts this only for a certificate valid for 14 days or less (an ECDSA P-256 key); otherwise it is ignored.
      if (M.moqWasmWtCertHashes) options.serverCertificateHashes = M.moqWasmWtCertHashes;
      c.wt = new WebTransport(url, options);
    } catch (e) {
      setTimeout(function () { if (!c.dead) M._moq_wasm_wt_on_failed(m, 1); }, 0);
      return;
    }
    c.wt.ready.then(function () {
      if (c.dead) return;
      var dgramMax = (c.wt.datagrams && c.wt.datagrams.maxDatagramSize) || 0;
      if (dgramMax) c.dgw = c.wt.datagrams.writable.getWriter();
      var pp = cstr(c.wt.protocol || '');
      M._moq_wasm_wt_on_ready(m, pp, dgramMax ? 1 : 0);
      M._moq_wasm_wt_free(pp);
      if (c.dead) return;
      acceptLoop(c.wt.incomingUnidirectionalStreams, 0);
      acceptLoop(c.wt.incomingBidirectionalStreams, 1);
      if (dgramMax) datagramLoop();
    }, function () { if (!c.dead) M._moq_wasm_wt_on_failed(m, 2); });
    c.wt.closed.then(function (info) {
      console.warn('[moq wasm_wt] connection closed: code=' + (info && info.closeCode) + ' reason=' + JSON.stringify(info && info.reason));
      if (!c.dead) M._moq_wasm_wt_on_closed(m, (info && info.closeCode) | 0, 1);
    }, function (e) {
      console.warn('[moq wasm_wt] connection failed: ' + e + ' (url ' + url + ', protocols ' + JSON.stringify(protocols) + ', pinned certificate ' + (M.moqWasmWtCertHashes ? 'yes' : 'no') + ')');
      if (!c.dead) M._moq_wasm_wt_on_closed(m, 0, 0);
    });
  },

  moq_wasm_wt_js_open_stream: function (m, bidi) {
    var c = Module.moqWasmWt.conns[m];
    var id = c.nextId++;
    var e = { w: null, r: null, chain: null };
    c.streams[id] = e;
    var create = bidi ? c.wt.createBidirectionalStream() : c.wt.createUnidirectionalStream();
    e.chain = create.then(function (s) {
      if (bidi) {
        e.w = s.writable.getWriter();
        // The peer's half of a bidirectional stream we opened.
        (async function () {
          var r = s.readable.getReader();
          e.r = r;
          try {
            for (;;) {
              var res = await r.read();
              if (c.dead) return;
              var u8 = res.done ? new Uint8Array(0) : res.value;
              var p = Module._moq_wasm_wt_alloc(u8.length);
              HEAPU8.set(u8, p);
              Module._moq_wasm_wt_on_stream_bytes(m, id, 1, p, u8.length, res.done ? 1 : 0);
              Module._moq_wasm_wt_free(p);
              if (res.done) return;
            }
          } catch (err) {
            if (!c.dead) Module._moq_wasm_wt_on_stream_reset(m, id, (err && err.streamErrorCode) | 0);
          }
        })();
      } else {
        e.w = s.getWriter();
      }
    }).catch(function () {
      if (!c.dead) Module._moq_wasm_wt_on_stream_reset(m, id, 0);
    });
    return id;
  },

  moq_wasm_wt_js_write: function (m, id, p, len, fin) {
    var c = Module.moqWasmWt.conns[m];
    var e = c && c.streams[id];
    if (!e) return;
    var data = HEAPU8.slice(p, p + len);
    e.chain = e.chain.then(async function () {
      if (!e.w) return;
      if (len) await e.w.write(data);
      if (fin) await e.w.close();
    }).catch(function () { /* a failed stream is reported through its reader or the close */ });
  },

  moq_wasm_wt_js_reset: function (m, id, code) {
    var c = Module.moqWasmWt.conns[m];
    var e = c && c.streams[id];
    if (!e) return;
    // Resetting discards queued writes: abort now when the stream exists.
    if (e.w) { try { e.w.abort(code); } catch (err) {} }
    else e.chain = e.chain.then(function () { if (e.w) return e.w.abort(code); });
  },

  moq_wasm_wt_js_stop: function (m, id, code) {
    var c = Module.moqWasmWt.conns[m];
    var e = c && c.streams[id];
    if (e && e.r) { try { e.r.cancel(code); } catch (err) {} }
  },

  moq_wasm_wt_js_datagram: function (m, p, len) {
    var c = Module.moqWasmWt.conns[m];
    if (!c || !c.dgw) return;
    c.dgw.write(HEAPU8.slice(p, p + len)).catch(function () {});
  },

  moq_wasm_wt_js_datagram_max: function (m) {
    var c = Module.moqWasmWt.conns[m];
    return (c && c.wt && c.wt.datagrams && c.wt.datagrams.maxDatagramSize) | 0;
  },

  moq_wasm_wt_js_close: function (m, code) {
    var c = Module.moqWasmWt.conns[m];
    if (c && c.wt) { try { c.wt.close({ closeCode: code }); } catch (err) {} }
  },

  moq_wasm_wt_js_schedule: function (m, delayMs) {
    var c = Module.moqWasmWt.conns[m];
    if (!c || c.dead) return;
    var at = performance.now() + delayMs;
    if (c.timer !== null && c.timerAt <= at) return;     // an earlier wake is already pending
    if (c.timer !== null) clearTimeout(c.timer);
    c.timerAt = at;
    c.timer = setTimeout(function () {
      c.timer = null;
      if (!c.dead) Module._moq_wasm_wt_on_timer(m);
    }, delayMs);
  },

  moq_wasm_wt_js_release: function (m) {
    var conns = Module.moqWasmWt && Module.moqWasmWt.conns;
    var c = conns && conns[m];
    if (!c) return;
    c.dead = true;
    if (c.timer !== null) clearTimeout(c.timer);
    if (c.wt) { try { c.wt.close(); } catch (err) {} }
    delete conns[m];
  },
});
