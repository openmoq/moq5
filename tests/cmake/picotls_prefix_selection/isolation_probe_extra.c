/* The isolation probe's intended warning plus an unrelated one
 * (diagnostic-gate control). */
#warning moq-isolation-probe
#warning moq-unrelated-warning
int moq_isolation_probe_extra(void) { return 0; }
