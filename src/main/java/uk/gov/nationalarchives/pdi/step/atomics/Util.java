/**
 * The MIT License
 * Copyright © 2021 The National Archives
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package uk.gov.nationalarchives.pdi.step.atomics;

import javax.annotation.Nullable;

public interface Util {

    /**
     * Given a String, returns null if the String is empty, or
     * otherwise the String.
     *
     * @param s a String
     *
     * @return a non-empty String, else null
     */
    static @Nullable String nullIfEmpty(@Nullable final String s) {
        if (s != null && !s.isEmpty()) {
            return s;
        } else {
            return null;
        }
    }

    /**
     * Given a String, returns an empty String if the string is null, or
     * otherwise the String.
     *
     * @param s a String
     *
     * @return an empty String if {@code s == null}, else a string.
     */
    static String emptyIfNull(@Nullable final String s) {
        if (s == null) {
            return "";
        } else {
            return s;
        }
    }

    /**
     * Returns true if the String is either null or empty.
     *
     * @param s a String
     *
     * @return true if the String is null or empty, false otherwise.
     */
    static boolean isNullOrEmpty(@Nullable final String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Returns true if the String is not-empty (or null).
     *
     * @param s a String
     *
     * @return true if the String is not null and not empty, false otherwise.
     */
    static boolean isNotEmpty(@Nullable final String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Determine if the string is `true` or `false`.
     *
     * @return true if the string is `true` or `false`, otherwise false
     */
    static boolean isStrTrueOrFalse(final String s) {
        if (s == null) {
            return false;
        }
        return s.equals("true") || s.equals("false");
    }

    /**
     * Attempt to create a String of `true` or `false` from
     * and unknown string.
     *
     * @return `true` if the string is `true` or a number greater than 0, else `false`
     */
    static String unknownStrToBooleanStr(String s) {
        if (s == null) {
            return "false";
        }
        s = s.toLowerCase();
        if (!isStrTrueOrFalse(s)) {
            try {
                // attempt conversion from integer
                s = Integer.parseInt(s) > 0 ? "true" : "false";
            } catch (final NumberFormatException e) {
                s = "false";
            }
        }
        return s;
    }

    /**
     * Attempt to create a String of a number from
     * and unknown string.
     *
     * @return a number as a String if the string is a number or is `true`, otherwise `0`
     */
    static String unknownStrToIntegerStr(String s) {
        if (s == null) {
            return "0";
        }
        s = s.toLowerCase();
        try {
            Integer.parseInt(s);
        } catch (final NumberFormatException e) {
            s = s.equals("true") ? "1" : "0";
        }
        return s;
    }
}
