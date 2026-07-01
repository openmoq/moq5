import Foundation
import MoQRecvArgs

do {
    let args = try RecvArgs.parse(CommandLine.arguments)

    if args.demo {
        writeStderr("Use moq-swift-recv for --demo mode.")
        writeStderr("moq-swift-recv-live is for live transport only.")
        Foundation.exit(1)
    }

    try runLive(args: args)
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
