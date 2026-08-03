/*
 * Private test seam for the managed proxygen facade's pump scheduler. Exists
 * ONLY in a build with MOQ_PROXYGEN_WT_TESTING defined (the scheduler test
 * recompiles wt_managed.cpp with it). NEVER compiled into the shipped adapter
 * and never exported.
 *
 * Starts a managed facade whose network thread, instead of dialing, builds a
 * real libmoq session + attach Adapter over the caller's (fake) WebTransport and
 * then drives the PRODUCTION pump_once/schedule_pump_soon loop. The WebTransport
 * must outlive the facade (the normal stop/destroy tears the adapter down first).
 * Returns the started facade (use the public stop/destroy), or nullptr on error.
 */
#ifndef MOQ_PROXYGEN_WT_MANAGED_TESTING_H
#define MOQ_PROXYGEN_WT_MANAGED_TESTING_H

#include <moq/proxygen_wt_managed.h>

namespace proxygen { class WebTransport; }

moq_proxygen_wt_managed_t *moq_proxygen_wt_test_start(
    const moq_proxygen_wt_managed_cfg_t *cfg, proxygen::WebTransport *fake_wt);

#endif
