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

import com.evolvedbinary.j8fu.tuple.Tuple2;
import net.jcip.annotations.ThreadSafe;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.evolvedbinary.j8fu.tuple.Tuple.*;

/**
 * In-memory storage for Atomic Values.
 *
 * Basically just a simple wrapper around a {@link ConcurrentHashMap}.
 *
 * This class follows a singleton-pattern and so
 * there is only ever one instance per-JVM.
 */
@ThreadSafe
public class AtomicStorage {

    public static final AtomicStorage INSTANCE = new AtomicStorage();

    private final ConcurrentHashMap<String, Tuple2<AtomicType, Object>> storage;

    private AtomicStorage() {
        this.storage = new ConcurrentHashMap<>();
    }

    /**
     * Get an Atomic Value from Storage.
     *
     * @param id the identifier of the Atomic Value
     * @param atomicType the type of the Atomic Value
     *
     * @return null if there is no such atomic with the provided id, otherwise the AtomicValue
     *
     * @throws IllegalArgumentException if the the Atomic Value exists but has a different AtomicType to that which was requested
     */
    public @Nullable Object getAtomic(final String id, final AtomicType atomicType) throws IllegalStateException {
        final Tuple2<AtomicType, Object> typedAtomic = storage.get(id);
        if (typedAtomic == null) {
            return null;
        }

        if (atomicType != typedAtomic._1) {
            throw new IllegalArgumentException("Requested type: " + atomicType + " but found type: " + typedAtomic._1 + " for id: " + id);
        }

        return typedAtomic._2;
    }

    /**
     * Get an Atomic Value from Storage,
     * or Create it if there is no existing Atomic Value.
     *
     * @param id the identifier of the Atomic Value
     * @param atomicType the type of the Atomic Value
     * @param initialValue the initial value for the Atomic Value if it is created
     *
     * @return the existing or newly created AtomicValue
     *
     * @throws IllegalArgumentException if the the Atomic Value exists but has a different AtomicType to that which was requested
     */
    public Object getOrCreateAtomic(final String id, final AtomicType atomicType, final String initialValue) throws IllegalStateException {
        return storage.compute(id, (k, v) -> {
            if (v != null) {
                // get
                if (atomicType != v._1) {
                    throw new IllegalArgumentException("Requested type: " + atomicType + " but found type: " + v._1 + " for id: " + id);
                }
                return v;

            } else {
                // create
                atomicType.checkValidValue(initialValue);
                switch (atomicType) {
                    case Boolean:
                        return Tuple(atomicType, new AtomicBoolean(Boolean.valueOf(initialValue)));
                    case Integer:
                        return Tuple(atomicType, new AtomicInteger(Integer.valueOf(initialValue)));
                    default:
                        throw new IllegalArgumentException("No such AtomicType: " + atomicType);
                }
            }
        })._2;
    }

    public boolean removeAtomic(final String id) {
        return storage.remove(id) != null;
    }

    /**
     * Removes all Atomic Booleans from storage.
     *
     * Used for testing!
     */
    void clear() {
        storage.clear();
    }

    /**
     * Gets a copy of the storage.
     *
     * Used for testing!
     *
     * @return a copy of the storage map
     */
    Map<String, Tuple2<AtomicType, Object>> copy() {
        return new HashMap<>(storage);
    }

    /**
     * Sets the storage.
     *
     * This function is NOT thread-safe!
     *
     * Used for testing!
     *
     * @param atomicValues the values to set the storage to
     */
    void set(final Map<String, Tuple2<AtomicType, Object>> atomicValues) {
        storage.clear();
        storage.putAll(atomicValues);
    }
}
