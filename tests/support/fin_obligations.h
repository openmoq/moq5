#ifndef MOQ_TEST_FIN_OBLIGATIONS_H
#define MOQ_TEST_FIN_OBLIGATIONS_H

/*
 * Bounded, test-only ledger of the FINs a transport still owes.
 *
 * A write interposer reports every FIN-carrying write to the ledger: a
 * refused one opens an obligation for its stream, and a later taken one
 * discharges it. Retries of an owed FIN share their stream's entry, so the
 * table bounds the streams owing a FIN at once, not the attempts; a refusal
 * that finds the table full fails the ledger rather than going untracked. A
 * FIN taken discharges only an obligation already open, so one taken before
 * its stream's refusal settles nothing.
 *
 * fin_obligations_controls() drives the ledger's controls through the
 * consumer's own interposer path, so they cover its wiring as well.
 *
 * NOT installed.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define FIN_OBLIGATIONS_MAX 64     /* streams owing a FIN at once */

typedef struct {
    uint64_t owed[FIN_OBLIGATIONS_MAX];  /* streams whose FIN is owed */
    size_t   n_owed;
    size_t   peak;          /* most streams owing at once */
    bool     overflow;      /* a refusal found the table full */
    uint32_t refusals;      /* FIN-carrying writes refused, retries included */
    uint32_t opened;        /* obligations opened */
    uint32_t discharged;    /* obligations a later taken FIN closed */
} fin_obligations_t;

/* A FIN-carrying write on `sid` was taken, or refused. */
static inline void fin_obligations_note(fin_obligations_t *f, uint64_t sid,
                                        bool taken)
{
    size_t i = 0;
    while (i < f->n_owed && f->owed[i] != sid) i++;
    if (taken) {
        if (i < f->n_owed) {
            f->owed[i] = f->owed[--f->n_owed];
            f->discharged++;
        }
        return;
    }
    f->refusals++;
    if (i < f->n_owed) return;     /* a retry of a FIN already owed */
    if (f->n_owed == FIN_OBLIGATIONS_MAX) {
        f->overflow = true;
        return;
    }
    f->owed[f->n_owed++] = sid;
    f->opened++;
    if (f->n_owed > f->peak) f->peak = f->n_owed;
}

/* Every refused FIN was taken later, and none went untracked. */
static inline bool fin_obligations_settled(const fin_obligations_t *f)
{
    return !f->overflow && f->n_owed == 0;
}

/* The ledger's controls, each from an empty `f` and driven through `note`:
 * the consumer's path from a FIN-carrying write's result into `f`, `len`
 * being the bytes the write carried with its FIN. Returns the controls that
 * failed to see what they must, bit k for the k-th in order (0 when none);
 * leaves `f` empty. */
static inline unsigned fin_obligations_controls(
    fin_obligations_t *f, void (*note)(uint64_t sid, size_t len, bool taken))
{
    unsigned failed = 0;

    /* More retries on one stream than a 256-entry attempt log holds, then
     * another stream's FIN refused and never taken: that one is owed. */
    memset(f, 0, sizeof(*f));
    for (int i = 0; i < 300; i++) note(4, 0, false);
    note(4, 0, true);
    note(8, 0, false);
    if (fin_obligations_settled(f) || f->n_owed != 1 || f->owed[0] != 8)
        failed |= 1u << 0;

    /* A FIN taken before its stream's refusal does not discharge it. */
    memset(f, 0, sizeof(*f));
    note(12, 0, true);
    note(12, 0, false);
    if (fin_obligations_settled(f)) failed |= 1u << 1;

    /* A refused write carrying bytes with its FIN owes the FIN too. */
    memset(f, 0, sizeof(*f));
    note(16, 1000, false);
    if (fin_obligations_settled(f)) failed |= 1u << 2;

    /* One stream more than the table holds fails the ledger, even with
     * every FIN taken later. */
    memset(f, 0, sizeof(*f));
    for (uint64_t s = 0; s <= FIN_OBLIGATIONS_MAX; s++) note(4 * s, 0, false);
    for (uint64_t s = 0; s <= FIN_OBLIGATIONS_MAX; s++) note(4 * s, 0, true);
    if (fin_obligations_settled(f) || !f->overflow) failed |= 1u << 3;

    /* Refused and retried, then taken, with or without bytes: settled. */
    memset(f, 0, sizeof(*f));
    note(20, 0, false);
    note(20, 0, false);
    note(20, 0, true);
    note(24, 1000, false);
    note(24, 0, true);
    if (!fin_obligations_settled(f) || f->refusals != 3 || f->opened != 2 ||
        f->discharged != 2)
        failed |= 1u << 4;

    memset(f, 0, sizeof(*f));
    return failed;
}

#endif /* MOQ_TEST_FIN_OBLIGATIONS_H */
