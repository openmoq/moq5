# MoQ publisher sample

An acceptance app for the `:moq5-android` send side. It has two starts:

- **Start synthetic** publishes a finite run of synthetic H.264 frames: the
  publish lifecycle, with no camera and no permission.
- **Start live camera + mic** asks for the CAMERA and RECORD_AUDIO permissions,
  then publishes the back camera as H.264 (720p30) and the microphone as AAC-LC
  (48 kHz mono) under one namespace, with a catalog, until you press Stop.

Both use the relay URL and namespace fields. The track name and frame count
apply only to the synthetic run. While either runs, the first button is Stop.

The live path needs API 24 or later, a back camera and a microphone. On a
device without them it refuses by name before connecting.

## Trust

The endpoint uses the library's platform trust (`TrustPolicy.PlatformTrustManager`):
the relay's certificate must chain to a trusted anchor and must match the URL's
host or IP address. There is no switch that turns verification off.

**Debug builds** add one thing, in `src/debug/`: a Network Security Config
whose `debug-overrides` section also trusts CAs from the device's **user**
store. The platform applies `debug-overrides` only to a debuggable app, and a
release build carries neither the config nor the manifest attribute.

This broadens trust for the debug app. Any CA a user has installed is also an
anchor, and debug-override anchors override pin sets. So do this on a
dedicated emulator, with a CA you created for this purpose.

## Publishing to your own relay from the Android emulator

The emulator reaches your machine's loopback as `10.0.2.2`, so the relay's
certificate must name that IP address. The steps below need OpenSSL 3 and
`adb`. Every key stays in `$MOQ5_DEV` on your machine, and only the CA's public
certificate goes to the emulator.

### 1. Create a local CA and the relay's certificate

```sh
export MOQ5_DEV=~/.moq5-dev
mkdir -p "$MOQ5_DEV" && chmod 700 "$MOQ5_DEV" && cd "$MOQ5_DEV"

# A fresh CA, valid for 30 days.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
    -subj "/CN=moq5 local development CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout ca.key -out ca.pem

# The relay's certificate, for the emulator's view of the host and the host itself.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=moq5 local relay" -keyout relay.key -out relay.csr
printf '%s\n' \
    'basicConstraints=critical,CA:FALSE' \
    'keyUsage=critical,digitalSignature' \
    'extendedKeyUsage=serverAuth' \
    'subjectAltName=IP:10.0.2.2,IP:127.0.0.1' > relay.ext
openssl x509 -req -in relay.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days 30 -extfile relay.ext -out relay.pem
chmod 600 ca.key relay.key
```

### 2. Check the certificate

```sh
openssl verify -CAfile ca.pem -purpose sslserver -verify_ip 10.0.2.2 relay.pem
# relay.pem: OK
openssl x509 -in relay.pem -noout -ext subjectAltName
# IP Address:10.0.2.2, IP Address:127.0.0.1
```

`-verify_ip 10.0.2.3` must fail with "IP address mismatch", and leaving out
`-CAfile` must fail with "unable to get local issuer certificate". The app
makes those same two checks.

### 3. Run your own relay

Build `moq5-relay` from this repository, following `tools/moq5-relay/README.md`
(it needs MsQuic):

```sh
export MOQ5_REPO=/path/to/this/checkout
cmake -S "$MOQ5_REPO" -B "$MOQ5_REPO/build-relay" \
    -DMOQ_BUILD_RELAY=ON -DMOQ_BUILD_ADAPTER_MSQUIC=ON -DMOQ_BUILD_MSQUIC_MANAGED=ON
cmake --build "$MOQ5_REPO/build-relay" --target moq5-relay
export MOQ5_RELAY="$MOQ5_REPO/build-relay/tools/moq5-relay/moq5-relay"
```

Then write its configuration next to the certificate and start it:

```sh
cd "$MOQ5_DEV"
python3 - <<'PY'
import json, os
config = json.load(open(os.environ["MOQ5_REPO"] + "/tools/moq5-relay/examples/relay.json"))
config["listener"].update(host="127.0.0.1", port=4443, versions=[18, 16], cert="relay.pem", key="relay.key")
json.dump(config, open("relay.json", "w"), indent=2)
PY
"$MOQ5_RELAY" capacity --config relay.json   # resolves the configuration, starts nothing
"$MOQ5_RELAY" serve --config relay.json
# MOQ5 Relay: listening on 127.0.0.1:4443 (moqt-18+moqt-16), 2 lanes
```

If UDP port 4443 is already in use (`lsof -nP -iUDP:4443`), choose another
port, and use the same port in the app's URL.

### 4. Trust the CA on the emulator

Use a dedicated AVD, with the back camera set to *Emulated* or *VirtualScene*
and the virtual microphone using the host's audio input.

```sh
adb push "$MOQ5_DEV/ca.pem" /sdcard/Download/moq5-local-ca.crt
```

On the emulator, open Settings → Security (on newer versions: Security &
privacy → More security settings) → Encryption & credentials → Install a
certificate → **CA certificate**, and choose `moq5-local-ca.crt`. It then
appears under Trusted credentials → **User** as "moq5 local development CA".

### 5. Install the debug build and publish

```sh
tools/gradlew-verified :samples:publisher:assembleDebug          # from bindings/kotlin
adb install -r samples/publisher/build/outputs/apk/debug/publisher-debug.apk
```

Leave the relay URL at its default, `moqt://10.0.2.2:4443` (or change the port
you chose), pick a namespace, and press **Start live camera + mic**. Allow both
permissions. A denial is reported by name, and nothing starts.

### Clean up

Remove the CA under Trusted credentials → User on the emulator, stop the relay,
and delete `$MOQ5_DEV`.
