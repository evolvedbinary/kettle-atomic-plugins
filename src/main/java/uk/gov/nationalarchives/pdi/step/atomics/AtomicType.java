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

/**
 * The types of Atomic Value.
 */
public enum AtomicType {
    Boolean,
    Integer;

    /**
     * Checks whether the provided value string is a valid
     * value for the AtomicType.
     *
     * @param value the value to test
     * @return the input value to test if valid
     * @throws IllegalArgumentException if the value is not valid for the AtomicType
     */
    public String checkValidValue(final String value) throws IllegalArgumentException {
        if (this == AtomicType.Integer) {
            if (value == null) {
                throw new IllegalArgumentException("null is not a valid integer");
            }
            java.lang.Integer.valueOf(value);
        } else {
            if (value == null) {
                throw new IllegalArgumentException("null is not a valid boolean");
            }
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                throw new IllegalArgumentException("'" + value + "' is not a valid boolean");
            }
        }

        return value;
    }
}
