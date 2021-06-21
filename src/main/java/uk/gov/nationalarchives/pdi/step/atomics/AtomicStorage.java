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

import com.evolvedbinary.j8fu.function.ConsumerE;
import com.evolvedbinary.j8fu.function.FunctionE;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory storage for Atomic Values.
 *
 * Basically just a simple wrapper around a {@link Map}.
 *
 * This class follows a singleton-pattern and so
 * there is only ever one instance per-JVM.
 */
@ThreadSafe
public class AtomicStorage {

    public static final AtomicStorage INSTANCE = new AtomicStorage();

    private final Storage storage;

    private AtomicStorage() {
        this.storage = new Storage();
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
    public @Nullable AtomicValue getAtomic(final String id, final AtomicType atomicType) throws IllegalStateException {
        final AtomicValue atomicValue = storage.read(store -> store.get(id));

        if (atomicValue == null) {
            return null;
        }

        if (atomicType != atomicValue.getType()) {
            throw new IllegalArgumentException("Requested type: " + atomicType + " but found type: " + atomicValue.getType() + " for id: " + id);
        }

        return atomicValue;
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
    public AtomicValue getOrCreateAtomic(final String id, final AtomicType atomicType, final String initialValue) throws IllegalStateException {

        // 1) optimistically try and get the value
        final AtomicValue existingAtomic = storage.read(store -> store.get(id));
        if (existingAtomic != null) {
            if (atomicType != existingAtomic.getType()) {
                throw new IllegalArgumentException("Requested type: " + atomicType + " but found type: " + existingAtomic.getType() + " for id: " + id);
            }
            return existingAtomic;
        }

        // 2) no such value, lock for write
        return storage.write(store -> {

            // 2.1) we must try and read the value again as this
            // thread may have been preempted between releasing
            // the read lock and taking the write lock
            AtomicValue atomic = store.get(id);
            if (atomic != null) {
                if (atomicType != atomic.getType()) {
                    throw new IllegalArgumentException("Requested type: " + atomicType + " but found type: " + atomic.getType() + " for id: " + id);
                }
                return atomic;
            }

            // 3) still no value, so create one
            atomicType.checkValidValue(initialValue);
            switch (atomicType) {
                case Boolean:
                    atomic = new AtomicBooleanValue(Boolean.parseBoolean(initialValue));
                    break;

                case Integer:
                    atomic = new AtomicIntegerValue(Integer.parseInt(initialValue));
                    break;

                default:
                    throw new IllegalArgumentException("No such AtomicType: " + atomicType);
            }

            // 3.1) store the atomic
            store.put(id, atomic);
            return atomic;
        });
    }

    public boolean removeAtomic(final String id) {
        return storage.write(store -> store.remove(id)) != null;
    }

    /**
     * Removes all Atomic Booleans from storage.
     *
     * Used for testing!
     */
    void clear() {
        storage.update(Map::clear);
    }

    /**
     * Gets a copy of the storage.
     *
     * Used for testing!
     *
     * @return a copy of the storage map
     */
    Map<String, AtomicValue> copy() {
        return storage.read(HashMap::new);
    }

    /**
     * Sets the storage.
     *
     * Used for testing!
     *
     * @param atomicValues the values to set the storage to
     */
    void set(final Map<String, AtomicValue> atomicValues) {
        storage.update(store -> {
            store.clear();
            store.putAll(atomicValues);
        });
    }

    /**
     * Put the atomic value in the storage.
     *
     * Used for testing!
     *
     * @param id the identifier of the Atomic Value
     * @param atomicValue the Atomic Value
     *
     * @return the previous atomic value associated with the id
     */
    @Nullable AtomicValue put(final String id, final AtomicValue atomicValue) {
        return storage.write(store -> store.put(id, atomicValue));
    }

    @ThreadSafe
    private static class Storage {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        @GuardedBy("lock")
        private final Map<String, AtomicValue> store = new HashMap<>();

        <T, E extends Throwable> T read(final FunctionE<Map<String, AtomicValue>, T, E> reader) throws E {
            this.lock.readLock().lock();
            try {
                return reader.apply(store);
            } finally {
                this.lock.readLock().unlock();
            }
        }

        <T, E extends Throwable> T write(final FunctionE<Map<String, AtomicValue>, T, E> writer) throws E {
            this.lock.writeLock().lock();
            try {
                return writer.apply(store);
            } finally {
                this.lock.writeLock().unlock();
            }
        }

        <E extends Throwable> void update(final ConsumerE<Map<String, AtomicValue>, E> writer) throws E {
            this.lock.writeLock().lock();
            try {
                writer.accept(store);
            } finally {
                this.lock.writeLock().unlock();
            }
        }
    }
}
