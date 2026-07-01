#include "session_internal.h"

moq_result_t handle_start(moq_session_t *s)
{
    return s->profile->start(s);
}
