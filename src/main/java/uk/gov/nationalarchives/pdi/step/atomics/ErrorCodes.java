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
 * Error Codes which can be produced by the Steps
 * when an error occurs.
 */
public enum ErrorCodes {
    NO_SUCH_ATOMIC("NSA1", "No such Atomic Value"),
    NO_SUCH_ATOMIC_WAIT_TIMEOUT("NSA2", "Timeout reached when waiting for Atomic Value creation"),
    NO_SUCH_ATOMIC_WAIT_INTERRUPTED("NSA3", "Thread interrupted whilst waiting for Atomic Value creation"),
    AWAIT_ATOMIC_WAIT_INTERRUPTED("AWA4", "Thread interrupted whilst waiting for Atomic Value"),
    CAS_ATOMIC_WAIT_INTERRUPTED("CAS5", "Thread interrupted whilst waiting to CAS Atomic Value");

    private final String code;
    private final String description;

    ErrorCodes(final String code, final  String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
