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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UtilTest {

    @Test
    public void nullIfEmpty() {
        assertNull(Util.nullIfEmpty(null));
        assertNull(Util.nullIfEmpty(""));
        assertNotNull(Util.nullIfEmpty("a"));
        assertNotNull(Util.nullIfEmpty("abc"));
    }

    @Test
    public void emptyIfNull() {
        assertTrue(Util.emptyIfNull(null).isEmpty());
        assertTrue(Util.emptyIfNull("").isEmpty());
        assertFalse(Util.emptyIfNull("a").isEmpty());
        assertFalse(Util.emptyIfNull("abc").isEmpty());
    }

    @Test
    public void isNullOrEmptyString() {
        assertTrue(Util.isNullOrEmpty(null));
        assertTrue(Util.isNullOrEmpty(""));
        assertFalse(Util.isNullOrEmpty("a"));
        assertFalse(Util.isNullOrEmpty("abc"));
    }

    @Test
    public void isNotEmptyString() {
        assertFalse(Util.isNotEmpty(null));
        assertFalse(Util.isNotEmpty(""));
        assertTrue(Util.isNotEmpty("a"));
        assertTrue(Util.isNotEmpty("abc"));
    }

    @Test
    public void isStrTrueOrFalse() {
        assertFalse(Util.isStrTrueOrFalse(null));
        assertFalse(Util.isStrTrueOrFalse(""));
        assertFalse(Util.isStrTrueOrFalse("TRUE"));
        assertFalse(Util.isStrTrueOrFalse("FALSE"));
        assertFalse(Util.isStrTrueOrFalse("yes"));
        assertFalse(Util.isStrTrueOrFalse("no"));
        assertFalse(Util.isStrTrueOrFalse("YES"));
        assertFalse(Util.isStrTrueOrFalse("NO"));
        assertTrue(Util.isStrTrueOrFalse("true"));
        assertTrue(Util.isStrTrueOrFalse("false"));
    }

    @Test
    public void unknownStrToBooleanStr() {
        assertEquals("false", Util.unknownStrToBooleanStr(null));
        assertEquals("false", Util.unknownStrToBooleanStr("false"));
        assertEquals("false", Util.unknownStrToBooleanStr("FALSE"));
        assertEquals("false", Util.unknownStrToBooleanStr("no"));
        assertEquals("false", Util.unknownStrToBooleanStr("NO"));
        assertEquals("false", Util.unknownStrToBooleanStr("YES"));
        assertEquals("false", Util.unknownStrToBooleanStr("0"));
        assertEquals("false", Util.unknownStrToBooleanStr("-1"));

        assertEquals("true", Util.unknownStrToBooleanStr("TRUE"));
        assertEquals("true", Util.unknownStrToBooleanStr("true"));
        assertEquals("true", Util.unknownStrToBooleanStr("1"));
        assertEquals("true", Util.unknownStrToBooleanStr("123"));
    }

    @Test
    public void unknownStrToIntegerStr() {
        assertEquals("0", Util.unknownStrToIntegerStr(null));
        assertEquals("0", Util.unknownStrToIntegerStr("false"));
        assertEquals("0", Util.unknownStrToIntegerStr("FALSE"));
        assertEquals("0", Util.unknownStrToIntegerStr("no"));
        assertEquals("0", Util.unknownStrToIntegerStr("NO"));
        assertEquals("0", Util.unknownStrToIntegerStr("YES"));
        assertEquals("0", Util.unknownStrToIntegerStr("0"));

        assertEquals("-1", Util.unknownStrToIntegerStr("-1"));
        assertEquals("1", Util.unknownStrToIntegerStr("TRUE"));
        assertEquals("1", Util.unknownStrToIntegerStr("true"));
        assertEquals("1", Util.unknownStrToIntegerStr("1"));
        assertEquals("123", Util.unknownStrToIntegerStr("123"));
    }

}
