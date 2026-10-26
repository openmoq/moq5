# Kotlin source as CODE: comments and string literals removed.
#
# Every gate in this tree that judges Kotlin or Kotlin-DSL source reads it
# through here, because an oracle satisfied by a comment or by a quoted example
# is not an oracle. Sharing one stripper is deliberate: a second copy would be
# free to drift, and the failure mode of a WRONG stripper is silent -- it does
# not error, it just reports about the wrong text.
#
# It is a real state machine rather than a quote-toggle, because Kotlin has
# three constructs a toggle gets wrong, and all three appear in this project:
#
#   TEMPLATES   `"a:${pin("b")}"` -- the `${...}` is CODE, and it contains its
#               own string. A toggle treats the inner quote as the closing one
#               and inverts its idea of "inside a string" for the REST OF THE
#               FILE, so every later row reads inverted text.
#   RAW STRINGS `"""^android-(\d+)$"""` -- no backslash escapes, and a lone `"`
#               inside is ordinary text.
#   ESCAPES     `"\""` -- a quote that does not end the literal.
#
# The template frames nest, so the modes are kept on a stack with a brace depth
# per frame; `}` only returns to the enclosing string at depth zero.
#
# kotlin_code <file>   writes the code stream to stdout.
kotlin_code() {
    awk '
    function push(m) { stack[++sp] = m; depth[sp] = 0 }
    function pop()   { sp-- }
    BEGIN { sp = 0; mode = "CODE"; block = 0 }
    {
        line = $0; i = 1; n = length(line); out = ""
        while (i <= n) {
            c = substr(line, i, 1)
            two = substr(line, i, 2)
            three = substr(line, i, 3)

            if (block) {
                if (two == "*/") { block = 0; i += 2 } else { i++ }
                continue
            }

            if (mode == "STR") {
                if (c == "\\") { i += 2; continue }
                if (two == "${") { push("STR"); mode = "CODE"; i += 2; continue }
                if (c == "\"") { mode = "CODE"; i++; continue }
                i++
                continue
            }

            if (mode == "RAW") {
                if (three == "\"\"\"") { mode = "CODE"; i += 3; continue }
                if (two == "${") { push("RAW"); mode = "CODE"; i += 2; continue }
                i++
                continue
            }

            if (two == "//") break
            if (two == "/*") { block = 1; i += 2; continue }
            if (three == "\"\"\"") { mode = "RAW"; i += 3; continue }
            if (c == "\"") { mode = "STR"; i++; continue }
            if (c == "{" && sp > 0) { depth[sp]++; out = out c; i++; continue }
            if (c == "}" && sp > 0) {
                if (depth[sp] == 0) { mode = stack[sp]; pop(); i++; continue }
                depth[sp]--
                out = out c; i++; continue
            }
            out = out c
            i++
        }
        print out
    }' "$1"
}
