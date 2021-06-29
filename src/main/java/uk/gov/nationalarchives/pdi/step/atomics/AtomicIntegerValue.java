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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple wrapper around {@link AtomicInteger} to
 * allow us to have a common super-type with
 * {@link AtomicBooleanValue}.
 */
public class AtomicIntegerValue implements AtomicValue {

    private final AtomicInteger atomic;

    /**
     * Creates a new {@link AtomicInteger} with the given initial value.
     *
     * @param initialValue the initial value
     */
    public AtomicIntegerValue(final int initialValue) {
        this.atomic = new AtomicInteger(initialValue);
    }

    @Override
    public AtomicType getType() {
        return AtomicType.Integer;
    }

    /**
     * See {@link AtomicInteger#get()}.
     *
     * @return the current value
     */
    public int get() {
        return atomic.get();
    }

    /**
     * See {@link AtomicInteger#compareAndSet(int, int)}.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful.
     */
    public boolean compareAndSet(final int expect, final int update) {
       return atomic.compareAndSet(expect, update);
    }
}
