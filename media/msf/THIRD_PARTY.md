# Third-party dependencies

## vendor/json.h

- **Source**: https://github.com/sheredom/json.h
- **License**: Unlicense (public domain)
- **Usage**: Private implementation detail of moq-msf. Parses JSON
  catalog input into a read-only DOM. No `json_*` types are exposed
  in the public `<moq/msf.h>` API.
- **Allocation**: Single allocation via `json_parse_ex()` with a
  caller-provided allocator bridge to `moq_alloc_t`.
