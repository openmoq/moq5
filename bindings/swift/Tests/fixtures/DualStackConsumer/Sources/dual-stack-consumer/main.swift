import Foundation
import MoQ
import MoQService

/* Reference both stacks' always-linked entry points so both canary object
 * files are pulled: linking this binary MUST fail with a duplicate
 * moq_swift_stack_guard symbol. */
if #available(macOS 13.0, *) {
    _ = try? MoQEndpoint.connect(to: URL(string: "moqt://127.0.0.1:1")!)
}
_ = try? Session(configuration: .init(perspective: .client))
