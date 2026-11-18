/*
 * Cross-parser oracle for the properties corpus.
 *
 * Loads a file with the REAL java.util.Properties -- the same loader Gradle,
 * settings.gradle.kts and the Gradle wrapper use -- and prints, for each key,
 * a line of the form:
 *
 *     <key>\t<length>\t<hex of the UTF-8 bytes>
 *
 * Hex rather than the raw value so a trailing space or a decoded escape is
 * visible in a diff instead of being swallowed by the terminal. The shell
 * reader is compared against THIS, never the other way round.
 */
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class PropsEcho {
    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        try (InputStream in = new FileInputStream(args[0])) { p.load(in); }
        for (int i = 1; i < args.length; i++) {
            String v = p.getProperty(args[i]);
            if (v == null) { System.out.println(args[i] + "\t-1\t<absent>"); continue; }
            StringBuilder hex = new StringBuilder();
            for (byte b : v.getBytes(StandardCharsets.UTF_8))
                hex.append(String.format("%02x", b));
            System.out.println(args[i] + "\t" + v.length() + "\t" + hex);
        }
    }
}
