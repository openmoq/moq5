#!/usr/bin/env bash
#
# Assemble SimplePlayer into a runnable .app bundle.
#
# `swift build` produces a bare executable, not an app bundle: macOS won't
# activate it as a normal windowed app and iOS can't install it. This wraps the
# build output into a proper .app and — crucially — fetches the Mozilla CA trust
# bundle at build time, so no certificate chain is ever committed to the repo.
#
# Usage:
#   scripts/make-app.sh macos      -> build/SimplePlayer.app   (then: open it)
#   scripts/make-app.sh ios-sim    -> build/SimplePlayer.app   (then: simctl install)
#
# macOS needs a libmoq-service pkg-config prefix:
#   PKG_CONFIG_PATH=<prefix>/lib/pkgconfig scripts/make-app.sh macos
# iOS needs the binary xcframeworks first (see docs/ios-packaging.md).
#
set -euo pipefail

cd "$(dirname "$0")/.."                 # package root (Examples/SimplePlayer)
PLATFORM="${1:-macos}"
CONFIG="${CONFIG:-debug}"
APP="build/SimplePlayer.app"
BUNDLE_ID="org.openmoq.SimplePlayer"
CA_URL="https://curl.se/ca/cacert.pem"  # Mozilla's CA bundle, as curl ships it

fetch_ca() {                            # $1 = destination path
  echo "→ fetching CA bundle: $CA_URL"
  curl -fsSL "$CA_URL" -o "$1"
}

case "$PLATFORM" in
macos)
  : "${PKG_CONFIG_PATH:?set PKG_CONFIG_PATH to your libmoq-service prefix}"
  export MOQ_SERVICE=1
  swift build -c "$CONFIG"
  BIN="$(swift build -c "$CONFIG" --show-bin-path)/SimplePlayer"
  rm -rf "$APP"
  mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
  cp "$BIN" "$APP/Contents/MacOS/SimplePlayer"
  fetch_ca "$APP/Contents/Resources/cacert.pem"
  cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
	<key>CFBundleExecutable</key><string>SimplePlayer</string>
	<key>CFBundleIdentifier</key><string>$BUNDLE_ID</string>
	<key>CFBundleName</key><string>SimplePlayer</string>
	<key>CFBundlePackageType</key><string>APPL</string>
	<key>CFBundleShortVersionString</key><string>1.0</string>
	<key>CFBundleVersion</key><string>1</string>
	<key>LSMinimumSystemVersion</key><string>13.0</string>
	<key>NSHighResolutionCapable</key><true/>
	<key>NSPrincipalClass</key><string>NSApplication</string>
</dict></plist>
PLIST
  echo "✓ built $APP  —  run it:  open $APP"
  ;;
ios-sim)
  export MOQ_SERVICE=1 MOQ_SERVICE_IOS=1
  SDK="$(xcrun --sdk iphonesimulator --show-sdk-path)"
  TRIPLE="arm64-apple-ios16.0-simulator"
  swift build --triple "$TRIPLE" --sdk "$SDK" -c "$CONFIG"
  BIN="$(swift build --triple "$TRIPLE" --sdk "$SDK" -c "$CONFIG" --show-bin-path)/SimplePlayer"
  rm -rf "$APP"
  mkdir -p "$APP"
  cp "$BIN" "$APP/SimplePlayer"
  cp Info.plist "$APP/Info.plist"       # the committed iOS Info.plist
  fetch_ca "$APP/cacert.pem"
  echo "✓ built $APP  —  install it:  xcrun simctl install booted $APP"
  ;;
*)
  echo "usage: $0 [macos|ios-sim]" >&2
  exit 2
  ;;
esac
