# Injected as CMAKE_PROJECT_picotls_INCLUDE, as FindPicoquic injects LibMoQ's
# picotls project policy when picoquic builds picotls. The header selection
# FindPicoquic defines is not part of this model.
function(_moq_pq_ptls_headers_from_source source)
endfunction()
include("${MOQ_SOURCE}/cmake/MoqPicotlsProjectPolicy.cmake")
