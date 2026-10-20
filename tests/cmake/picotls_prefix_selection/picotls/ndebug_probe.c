/* Compiles only while NDEBUG is undefined. */
#ifdef NDEBUG
#error moq-ndebug-probe: NDEBUG is defined
#endif
int moq_ndebug_probe(void) { return 0; }
