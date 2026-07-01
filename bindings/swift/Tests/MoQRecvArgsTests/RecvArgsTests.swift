import Testing
import MoQRecvArgs

@Suite("RecvArgs Parser")
struct RecvArgsTests {

    @Test("--help throws help error")
    func helpFlag() throws {
        #expect(throws: ArgError.help) {
            _ = try RecvArgs.parse(["bin", "--help"])
        }
    }

    @Test("-h throws help error")
    func shortHelp() throws {
        #expect(throws: ArgError.help) {
            _ = try RecvArgs.parse(["bin", "-h"])
        }
    }

    @Test("--demo succeeds without relay or namespace")
    func demoMode() throws {
        let args = try RecvArgs.parse(["bin", "--demo"])
        #expect(args.demo == true)
        #expect(args.relay.isEmpty)
    }

    @Test("--relay and --namespace parse correctly")
    func relayAndNamespace() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "moqt://host:4433",
            "--namespace", "a/b/c"
        ])
        #expect(args.relay == "moqt://host:4433")
        #expect(args.namespaceParts == ["a", "b", "c"])
    }

    @Test("--track overrides default")
    func customTrack() throws {
        let args = try RecvArgs.parse([
            "bin", "--demo", "--track", "my-catalog"
        ])
        #expect(args.catalogTrack == "my-catalog")
    }

    @Test("--buffer-ms parses value")
    func bufferMS() throws {
        let args = try RecvArgs.parse([
            "bin", "--demo", "--buffer-ms", "3000"
        ])
        #expect(args.bufferMS == 3000)
    }

    @Test("--buffer-ms 0 throws invalidValue")
    func bufferMSZero() throws {
        #expect(throws: ArgError.self) {
            _ = try RecvArgs.parse(["bin", "--demo", "--buffer-ms", "0"])
        }
    }

    @Test("--buffer-ms without value throws")
    func bufferMSMissing() throws {
        #expect(throws: ArgError.self) {
            _ = try RecvArgs.parse(["bin", "--demo", "--buffer-ms"])
        }
    }

    @Test("Unknown flag throws unknownFlag")
    func unknownFlag() throws {
        #expect(throws: ArgError.unknownFlag("--bogus")) {
            _ = try RecvArgs.parse(["bin", "--demo", "--bogus"])
        }
    }

    @Test("Missing --relay value throws")
    func missingRelayValue() throws {
        #expect(throws: ArgError.missingValue("--relay")) {
            _ = try RecvArgs.parse(["bin", "--relay"])
        }
    }

    @Test("Live mode without --relay throws")
    func liveWithoutRelay() throws {
        #expect(throws: ArgError.self) {
            _ = try RecvArgs.parse(["bin", "--namespace", "x"])
        }
    }

    @Test("Live mode without --namespace throws")
    func liveWithoutNamespace() throws {
        #expect(throws: ArgError.self) {
            _ = try RecvArgs.parse(["bin", "--relay", "moqt://host:4433"])
        }
    }

    @Test("--verbose flag")
    func verboseFlag() throws {
        let args = try RecvArgs.parse(["bin", "--demo", "--verbose"])
        #expect(args.verbose == true)
    }

    @Test("--tls-disable-verify flag")
    func tlsFlag() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "moqt://h:1", "--namespace", "x",
            "--tls-disable-verify"
        ])
        #expect(args.tlsDisableVerify == true)
    }

    @Test("Relay URL parses moqt://host:port")
    func relayURL() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "moqt://relay.example.com:4433",
            "--namespace", "x"
        ])
        let relay = try args.parseRelay()
        #expect(relay.host == "relay.example.com")
        #expect(relay.port == 4433)
    }

    @Test("Relay URL parses https://host:port")
    func relayHTTPS() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "https://host:443", "--namespace", "x"
        ])
        let relay = try args.parseRelay()
        #expect(relay.host == "host")
        #expect(relay.port == 443)
    }

    @Test("Relay URL parses IPv6 [::1]:4433")
    func relayIPv6() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "moqt://[::1]:4433", "--namespace", "x"
        ])
        let relay = try args.parseRelay()
        #expect(relay.host == "::1")
        #expect(relay.port == 4433)
    }

    @Test("Relay URL missing port throws")
    func relayMissingPort() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "moqt://host", "--namespace", "x"
        ])
        #expect(throws: ArgError.self) {
            _ = try args.parseRelay()
        }
    }

    @Test("Relay URL parses bare host:port")
    func relayBare() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "relay.local:4433", "--namespace", "x"
        ])
        let relay = try args.parseRelay()
        #expect(relay.host == "relay.local")
        #expect(relay.port == 4433)
    }

    @Test("Relay URL rejects unsupported scheme")
    func relayBadScheme() throws {
        let args = try RecvArgs.parse([
            "bin", "--relay", "http://host:80", "--namespace", "x"
        ])
        #expect(throws: ArgError.self) {
            _ = try args.parseRelay()
        }
    }

    @Test("--max-objects parses value")
    func maxObjects() throws {
        let args = try RecvArgs.parse([
            "bin", "--demo", "--max-objects", "25"
        ])
        #expect(args.maxObjects == 25)
    }

    @Test("--max-seconds parses value")
    func maxSeconds() throws {
        let args = try RecvArgs.parse([
            "bin", "--demo", "--max-seconds", "5"
        ])
        #expect(args.maxSeconds == 5)
    }

    @Test("--max-objects missing value throws")
    func maxObjectsMissing() throws {
        #expect(throws: ArgError.self) {
            _ = try RecvArgs.parse(["bin", "--demo", "--max-objects"])
        }
    }

    @Test("--max-seconds missing value throws")
    func maxSecondsMissing() throws {
        #expect(throws: ArgError.self) {
            _ = try RecvArgs.parse(["bin", "--demo", "--max-seconds"])
        }
    }

    @Test("--max-objects non-numeric throws")
    func maxObjectsNonNumeric() throws {
        #expect(throws: ArgError.self) {
            _ = try RecvArgs.parse(["bin", "--demo", "--max-objects", "abc"])
        }
    }

    @Test("Default values")
    func defaults() throws {
        let args = try RecvArgs.parse(["bin", "--demo"])
        #expect(args.catalogTrack == "catalog")
        #expect(args.bufferMS == 2000)
        #expect(args.verbose == false)
        #expect(args.tlsDisableVerify == false)
        #expect(args.maxObjects == 0)
        #expect(args.maxSeconds == 0)
    }
}
