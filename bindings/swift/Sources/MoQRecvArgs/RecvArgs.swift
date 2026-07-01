import Foundation

public struct ParsedRelay: Sendable, Equatable {
    public let host: String
    public let port: Int32
}

public struct RecvArgs: Sendable, Equatable {
    public var relay: String = ""
    public var namespaceParts: [String] = []
    public var catalogTrack: String = "catalog"
    public var tlsDisableVerify: Bool = false
    public var bufferMS: UInt64 = 2000
    public var verbose: Bool = false
    public var demo: Bool = false
    public var maxObjects: UInt64 = 0
    public var maxSeconds: UInt64 = 0

    public static func parse(_ args: [String]) throws -> RecvArgs {
        var result = RecvArgs()
        var i = 1
        while i < args.count {
            let arg = args[i]
            switch arg {
            case "--relay":
                i += 1
                guard i < args.count else { throw ArgError.missingValue("--relay") }
                result.relay = args[i]
            case "--namespace":
                i += 1
                guard i < args.count else { throw ArgError.missingValue("--namespace") }
                result.namespaceParts = args[i].split(separator: "/").map(String.init)
            case "--track":
                i += 1
                guard i < args.count else { throw ArgError.missingValue("--track") }
                result.catalogTrack = args[i]
            case "--tls-disable-verify":
                result.tlsDisableVerify = true
            case "--buffer-ms":
                i += 1
                guard i < args.count, let v = UInt64(args[i]), v > 0 else {
                    throw ArgError.invalidValue("--buffer-ms", "positive integer required")
                }
                result.bufferMS = v
            case "--verbose":
                result.verbose = true
            case "--demo":
                result.demo = true
            case "--max-objects":
                i += 1
                guard i < args.count, let v = UInt64(args[i]) else {
                    throw ArgError.invalidValue("--max-objects", "non-negative integer required")
                }
                result.maxObjects = v
            case "--max-seconds":
                i += 1
                guard i < args.count, let v = UInt64(args[i]) else {
                    throw ArgError.invalidValue("--max-seconds", "non-negative integer required")
                }
                result.maxSeconds = v
            case "--help", "-h":
                throw ArgError.help
            default:
                throw ArgError.unknownFlag(arg)
            }
            i += 1
        }

        if !result.demo {
            if result.relay.isEmpty {
                throw ArgError.missingRequired("--relay (or use --demo)")
            }
            if result.namespaceParts.isEmpty {
                throw ArgError.missingRequired("--namespace")
            }
        }
        return result
    }

    /// Parse relay URL: moqt://host:port, https://host:port,
    /// or bare host:port. Rejects unsupported schemes.
    public func parseRelay() throws -> ParsedRelay {
        var url = relay
        if url.hasPrefix("moqt://") {
            url = String(url.dropFirst("moqt://".count))
        } else if url.hasPrefix("https://") {
            url = String(url.dropFirst("https://".count))
        } else if url.contains("://") {
            throw ArgError.invalidValue("--relay", "unsupported scheme")
        }
        url = url.trimmingCharacters(in: CharacterSet(charactersIn: "/"))

        let host: String
        let portStr: String

        if url.hasPrefix("[") {
            guard let closeBracket = url.firstIndex(of: "]") else {
                throw ArgError.invalidValue("--relay", "malformed IPv6 address")
            }
            host = String(url[url.index(after: url.startIndex)..<closeBracket])
            let after = url[url.index(after: closeBracket)...]
            guard after.hasPrefix(":") else {
                throw ArgError.invalidValue("--relay", "missing port after IPv6 address")
            }
            portStr = String(after.dropFirst())
        } else if let colonIdx = url.lastIndex(of: ":") {
            host = String(url[..<colonIdx])
            portStr = String(url[url.index(after: colonIdx)...])
        } else {
            throw ArgError.invalidValue("--relay", "missing port in URL")
        }

        guard let port = Int32(portStr), port > 0, port <= 65535 else {
            throw ArgError.invalidValue("--relay", "invalid port: \(portStr)")
        }
        guard !host.isEmpty else {
            throw ArgError.invalidValue("--relay", "empty host")
        }
        return ParsedRelay(host: host, port: port)
    }
}

public enum ArgError: Error, CustomStringConvertible, Equatable, Sendable {
    case help
    case missingValue(String)
    case invalidValue(String, String)
    case unknownFlag(String)
    case missingRequired(String)

    public var description: String {
        switch self {
        case .help: return usage
        case .missingValue(let f): return "Missing value for \(f)"
        case .invalidValue(let f, let r): return "Invalid value for \(f): \(r)"
        case .unknownFlag(let f):  return "Unknown flag: \(f)"
        case .missingRequired(let f): return "Required: \(f)"
        }
    }
}

public let usage = """
Usage: moq-swift-recv [OPTIONS]

Options:
  --relay URL            MoQ relay URL (moqt://host:port)
  --namespace PARTS      Track namespace (slash-separated, required for live)
  --track NAME           Catalog track name (default: catalog)
  --tls-disable-verify   Skip TLS certificate verification
  --buffer-ms MS         Playback buffer in milliseconds (default: 2000)
  --verbose              Print extra diagnostics
  --demo                 Run in-process demo instead of connecting to relay
  --max-objects N        Stop after receiving N media objects (0 = unlimited)
  --max-seconds N        Stop after N seconds (0 = unlimited)
  --help                 Show this help

Transport note:
  SwiftPM builds core/media/playback from C source. The picoquic
  threaded adapter is CMake/external-dependency based. A future
  phase will add a system-library bridge for live transport.
"""
