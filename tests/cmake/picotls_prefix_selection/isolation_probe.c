/* Compiles only with NDEBUG defined (a Release build) and warnings that are
 * not errors: LibMoQ's picotls policy must not reach this directory. */
#ifndef NDEBUG
#error moq-isolation-probe: NDEBUG is not defined
#endif
#warning moq-isolation-probe
int moq_isolation_probe(void) { return 0; }
