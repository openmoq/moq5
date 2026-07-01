import Foundation
import MoQRecvArgs

func writeStderr(_ msg: String) {
    FileHandle.standardError.write(Data((msg + "\n").utf8))
}

do {
    let args = try RecvArgs.parse(CommandLine.arguments)

    if args.demo {
        try runDemo(args: args)
    } else {
        writeStderr("moq-swift-recv: this binary supports --demo mode only.")
        writeStderr("")
        writeStderr("For live transport, build moq-swift-recv-live with the")
        writeStderr("CMake-installed adapter:")
        writeStderr("")
        writeStderr("  export PKG_CONFIG_PATH=<install>/lib/pkgconfig")
        writeStderr("  swift run moq-swift-recv-live \\")
        writeStderr("    --relay moqt://host:port --namespace ns")
        Foundation.exit(1)
    }
} catch let e as ArgError {
    if case .help = e {
        print(e.description)
        Foundation.exit(0)
    }
    writeStderr("Error: \(e.description)")
    writeStderr("")
    writeStderr(usage)
    Foundation.exit(1)
} catch {
    writeStderr("Error: \(error)")
    Foundation.exit(1)
}
