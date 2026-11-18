package org.openmoq.moq5.r8fixture;

import java.util.ArrayList;
import java.util.List;

/**
 * A minimal Java source in the fixture.
 *
 * Its purpose is not the code: it exists so this module's javac warning policy
 * ({@code -Xlint:all -Werror}) is EXERCISED rather than merely configured. With
 * no Java source at all, removing that policy changes nothing observable and no
 * mutant can discriminate it.
 */
final class FixtureJavaProbe {
    private FixtureJavaProbe() {}

    static List<String> names() {
        List<String> out = new ArrayList<>();
        out.add("moq5 r8 fixture");
        return out;
    }
}
