# CMake component export ownership

The service and optional adapters retain their existing
`find_package(libmoq CONFIG REQUIRED COMPONENTS ...)` entry points, module entry
filenames and `moq::...` imported names. Each component has its own native CMake
export set; foreign dependencies stay in their provider's namespace and sets.
`cmake/MoqComponentExports.cmake` registers the actual owned target for both
`export(EXPORT ...)` and `install(EXPORT ...)`. It does not reconstruct imported
artifact properties or library identity. CMake 3.20 remains the minimum version.

`MOQ_BUILD_PACKAGE_EXPORTS` is a BOOL option, **ON by default**, including when
LibMoQ is added as a subdirectory. Set it to OFF explicitly for an embedded
project that supplies source dependencies without package exports. OFF retains
the actual in-tree targets, artifact/header installation and existing pkg-config
rules. It generates or installs no LibMoQ CMake config/version/target modules and
registers no private foreign build export. Reusing an ON build tree with OFF also
withdraws its exact known build-tree package modules even when a component is
simultaneously disabled. OFF does not advertise a CMake
package or promise that pkg-config replaces its dependency closure.

With exports ON, source dependencies injected by a caller must have their own
unambiguous provider-owned native build/install exports. The caller also owns
ordinary dependency-package discovery or supplies the consumer targets before
finding LibMoQ. There is no foreign-loader selector, target hiding, foreign export
into `moq::`, or automatic duplicate registration of caller-owned exports. Installed
IMPORTED dependencies keep their normal automatic discovery; LibMoQ-managed Pico
source uses its exact private provider build export as described below.

Build-tree exports describe every configured producer configuration. For a
Ninja Multi-Config producer configured with Debug and Release, both records exist
after generation. A consumer may configure before Release is built; a Release
consumer build then fails for the missing Release artifact. Building that
producer configuration makes the same consumer usable. Application
`CMAKE_MAP_IMPORTED_CONFIG_*` settings and normal CMake fallback for unspecified
or single-config consumers remain intact. An empty single-config producer records
NOCONFIG. Repeated find calls retain the same targets and configuration records.
Native build exports replace their configured records on regeneration; leftover
files from the previous manual fragment format are not included.

When LibMoQ adds PicoQUIC from source, it also registers a build export of
PicoQUIC's existing `picoquic-targets` set under `picoquic::`. Cold build-tree
consumers import that producer's configured provider artifacts, after resolving
external dependencies, instead of rebuilding or selecting another PicoQUIC.
This does not modify the foreign checkout, put its targets in LibMoQ's sets, or
install a replacement foreign package. Imported SDK discovery and installed
consumer dependency discovery retain their package ownership.

The intended consumer usage lives on the real owned targets: service threading,
facades and media; PicoHTTP public headers and installed linkage; mvfst usage
with a link-only fmt edge; and proxygen's c-ares search directory. Native CMake
preserves the targets' compile features and language requirements as well as
static/shared artifact kinds. The MsQuic managed component remains an
artifact-free INTERFACE facade over its attach library. Cold component lookups
discover Threads when the producer's actual static target exports that private
edge: direct MsQuic attach with managed implementation compiled in, and the
wtquic Network managed facade. Unmanaged MsQuic attach retains no Threads
requirement; consumer BUILD_SHARED_LIBS does not select this closure.

Install prefixes describe the one selected producer configuration. Native CMake
installs the stable module and its selected configuration fragment, including
configured names/postfixes and installed library identity. Install Debug and
Release into separate prefixes. This does not introduce a mixed-prefix SDK or
per-config pkg-config selector. Runtime and import-library destinations are
registered separately; source treatment of DLL metadata is not native Windows
qualification.

`FindPicoquic` deliberately leaves an in-tree picotls pkg-config library directory
unresolved under multi-config generators: no single configuration's output
directory is selected at configure time. Native exports do not guess a Release
directory, flatten those artifacts, or qualify that incomplete `.pc` closure.
Existing single-config pkg-config selection is unchanged.

The permanent regression remains `tests/cmake/ManualComponentExportsTest.cmake`:

- `service` uses the real product and public C/C++ service consumers.
- `picoquic` and `pico-wt` use actual caller-supplied source/SDK graphs and public
  initializers, with one public target per consumer. Cold service-only finds
  (including repeated find) link only `moq::service` in the same build/install
  rows, so explicit facade finds cannot mask service autoloading. Build-tree
  checks also require imported provider records at the producer's exact paths.
- `helper` uses tiny real libraries under the twelve existing artifact/module
  shapes and the artifact-free MsQuic facade. It retains the own-C++ component
  with a C-only static dependency, exercises differing build/install LINK_ONLY
  interfaces and LINK_DIRECTORIES, and checks per-config static C++ language
  metadata. A private NO_SONAME reference and, on Apple, an absolute build identity
  changed to @rpath on install check source identity semantics. Shared identity
  property checks cover Apple/Linux; other POSIX platforms retain portable
  import/link/run checks. This is native registration coverage, not runtime
  certification of the corresponding optional backends.
- `embedding` uses the actual core/service, C++ and relay graph with a tiny real
  source dependency owned by the superproject. OFF leaves that dependency
  unexported while C/C++ in-tree consumers build/run and artifacts/headers install
  without CMake modules. ON uses the provider's separate ordinary package for
  cold build/install consumers. Its ON-to-OFF transition disables service at the
  same time and checks withdrawal while core/C++ consumers stay usable. It also
  checks a precise
  ON refusal of an unexported source dependency. This is export ownership coverage,
  not a transport SDK or backend runtime claim.

- `msquic-attach` uses actual pinned MsQuic SDK headers/library, finding only
  adapter-msquic in fresh C/C++ build/install consumers. Managed-ON/OFF producers
  cover the actual static Threads edge and the unmanaged control; shared attach
  consumers must also remain free of a new Threads discovery requirement.
- `wtquic-network` uses an actual current-API installed Network SDK on Apple,
  finding only adapter-wtquic-network-managed. No consumer finds Threads or service
  first. These rows configure/link against real SDK inputs; they do not execute
  a transport or certify backend runtime. Missing SDK inputs are precise failures.
  The causal pre-correction mode requires native CMake generation to fail naming
  Threads::Threads, not a fabricated consumer error.

Parent-package consumer tests register only with package exports enabled.
In-tree/native/unit tests, independent regressions that create their own packages,
and pkg-config-only checks keep their own capability scopes. Rust installed-SDK
rows use libmoq-service.pc and retain their existing backend/SDK requirements;
disabling CMake exports alone does not disable them. No tests are marked Not Run
or disabled to represent the absent package capability.

Each requested static/shared row checks exact Debug/Release locations and kinds,
repeated find, a missing Release artifact followed by a successful retry, removed
and aliased native Release records, explicit application mapping, empty consumer
selection of a Release-only producer without stale Debug records, NOCONFIG and
separate selected-config install prefixes. Installed properties and resolved
TARGET_FILE values must match the full owned prefix/libdir/filename, including
canonical containment. A matching basename in the producer tree is insufficient.
Shared-image evidence parses actual dyld loaded-image or detected-glibc
`calling init` records and compares normalized exact paths. Search attempts do
not count. For glibc shared installed runs, including embedding consumers, the
fixture explicitly sets process LD_LIBRARY_PATH to only that test-owned
prefix/libdir, without appending ambient paths. It retains LD_DEBUG tracing and
the existing exact imported/loaded-image checks. Build-tree runs and product
RPATH are unchanged. This staged runtime setup does not establish a self-contained
runtime SDK. Other loaders have no loaded-image qualification claim.

The fixture retains child logs and rejects unexpected warnings. Its old-product
mode asserts the actual same-path/different-content service generation error;
a compiler or provider failure does not demonstrate that collision. The test
requires a fresh work path or an exact ownership marker left by this fixture
at that canonical path before deleting previous outputs. Existing unmarked
directories, symlink roots
and ancestors of the product source or fixture are refused. Multi-config
invocations omit CMAKE_BUILD_TYPE; normal single-config uses remain covered.

Native Windows/Visual Studio and optional backend runtime support remain separate
qualification requirements. The older stand-in picotls selection fixture keeps
its single-config adapter probes; the real export regression supplies distinct
multi-config evidence without relabeling excluded probes as passes.
