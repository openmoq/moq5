#include <moq/moq.h>
#include <fixture_provider.h>
int main(void)
{
    return moq_version_string() == 0 || issue37_provider_value() != 37;
}
