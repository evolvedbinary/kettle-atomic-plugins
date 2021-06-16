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

public class AtomicTypeTest {

    @Test
    public void checkValidTypeInteger() {
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Integer.checkValidValue(null)
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Integer.checkValidValue("")
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Integer.checkValidValue("abc")
        );

        assertEquals("-1234", AtomicType.Integer.checkValidValue("-1234"));
        assertEquals("-1", AtomicType.Integer.checkValidValue("-1"));
        assertEquals("0", AtomicType.Integer.checkValidValue("0"));
        assertEquals("1", AtomicType.Integer.checkValidValue("1"));
        assertEquals("1234", AtomicType.Integer.checkValidValue("1234"));

        assertThrows(IllegalArgumentException.class,
                () -> AtomicType.Integer.checkValidValue(Long.toString(Long.MAX_VALUE))
        );
    }

    @Test
    public void checkValidTypeBoolean() {
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue(null)
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue("")
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue("no")
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue("yes")
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue("NO")
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue("YES")
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue("0")
        );
        assertThrows(IllegalArgumentException.class, () ->
                AtomicType.Boolean.checkValidValue("1")
        );

        assertEquals("true", AtomicType.Boolean.checkValidValue("true"));
        assertEquals("false", AtomicType.Boolean.checkValidValue("false"));
        assertEquals("TRUE", AtomicType.Boolean.checkValidValue("TRUE"));
        assertEquals("FALSE", AtomicType.Boolean.checkValidValue("FALSE"));
    }
}
