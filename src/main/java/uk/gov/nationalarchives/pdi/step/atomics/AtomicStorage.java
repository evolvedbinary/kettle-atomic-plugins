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
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

import static com.evolvedbinary.j8fu.tuple.Tuple.*;

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

    private final StampedLock lock;
    @GuardedBy("lock")
    private final Map<String, Tuple2<AtomicType, Object>> store;

    private AtomicStorage() {
        this.lock = new StampedLock();
        this.store = new HashMap<>();
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
        Tuple2<AtomicType, Object> typedAtomic;

        // 1) try an optimistic read
        long stamp = lock.tryOptimisticRead();
        typedAtomic = store.get(id);

        if (!lock.validate(stamp)) {
            // 2) optimistic read failed, so attempt with full read-lock
            stamp = lock.readLock();
            try {
                typedAtomic = store.get(id);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        if (typedAtomic == null) {
            return null;
        }

        return checkTypeAndReturnValue(atomicType, id, typedAtomic);
    }

    private static Object checkTypeAndReturnValue(final AtomicType expectedAtomicType, final String id, final Tuple2<AtomicType, Object> typedAtomic) throws IllegalArgumentException {
        if (expectedAtomicType != typedAtomic._1) {
            throw new IllegalArgumentException("Requested type: " + expectedAtomicType + " but found type: " + typedAtomic._1 + " for id: " + id);
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
        Tuple2<AtomicType, Object> typedAtomic;

        // 1) try an optimistic read
        long writeStamp = 0;
        try {
            long readStamp = lock.tryOptimisticRead();
            typedAtomic = store.get(id);

            if (!lock.validate(readStamp)) {
                // 2) optimistic read failed, so attempt with full read-lock
                readStamp = lock.readLock();
                try {
                    typedAtomic = store.get(id);

                    if (typedAtomic != null) {
                        // optimistic read succeeded and there is a value
                        return checkTypeAndReturnValue(atomicType, id, typedAtomic);

                    } else {
                        // read succeeded and there is no value, so we must create a value (under a write-lock)
                        // we can try to upgrade the lock here as an optimisation, and create later (see below)
                        writeStamp = lock.tryConvertToWriteLock(readStamp);
                    }

                } finally {
                    if (writeStamp == 0) {
                        lock.unlockRead(readStamp);
                    }
                }
            }

            // 1.1) optimistic read succeeded
            if (typedAtomic != null) {
                // optimistic read succeeded and there is a value
                return checkTypeAndReturnValue(atomicType, id, typedAtomic);
            } else {
                // optimistic read succeeded and there is no value, so we must create a value (under a write-lock)
                // we can try to upgrade the write-lock here and create later (see below)
                writeStamp = lock.tryConvertToWriteLock(readStamp);
            }

            // 3. no such value, we should might have a write-lock (from above) and must now create the value (after checking thread preemption)

            // indicates if we upgraded a read-lock (or optimistic read) to a write-lock, i.e. there was no preemption between read/write calls
            final boolean readWriteConsistent = writeStamp != 0;

            // if we don't yet have a write-lock (from upgrading), we need one
            if (writeStamp == 0) {
                writeStamp = lock.writeLock();
            }

            // if we are not read/write consistent we need to try and `get` again due to thread preemption
            if (!readWriteConsistent) {
                typedAtomic = store.get(id);

                // if there is a value, we can return it
                if (typedAtomic != null) {
                    return checkTypeAndReturnValue(atomicType, id, typedAtomic);
                }
            }

            // no value still, so create it
            typedAtomic = createTypedAtomic(atomicType, initialValue);
            store.put(id, typedAtomic);
            return typedAtomic._2;

        } finally {
            // if we have a write lock we must unlock it
            if (writeStamp != 0) {
                lock.unlockWrite(writeStamp);
            }
        }
    }

    private Tuple2<AtomicType, Object> createTypedAtomic(final AtomicType atomicType, final String initialValue) throws IllegalArgumentException {
        atomicType.checkValidValue(initialValue);
        switch (atomicType) {
            case Boolean:
                return Tuple(atomicType, new AtomicBoolean(Boolean.parseBoolean(initialValue)));

            case Integer:
                return Tuple(atomicType, new AtomicInteger(Integer.parseInt(initialValue)));

            default:
                throw new IllegalArgumentException("No such AtomicType: " + atomicType);
        }
    }

    public boolean removeAtomic(final String id) {
        final long stamp = lock.writeLock();
        try {
            return store.remove(id) != null;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Removes all Atomic Booleans from storage.
     *
     * Used for testing!
     */
    void clear() {
        final long stamp = lock.writeLock();
        try {
           store.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Gets a copy of the storage.
     *
     * Used for testing!
     *
     * @return a copy of the storage map
     */
    Map<String, Tuple2<AtomicType, Object>> copy() {
        Map<String, Tuple2<AtomicType, Object>> copy;

        // 1) try an optimistic read
        long stamp = lock.tryOptimisticRead();
        copy = new HashMap<>(store);

        if (!lock.validate(stamp)) {
            // 2) optimistic read failed, so attempt with full read-lock
            stamp = lock.readLock();
            try {
                copy = new HashMap<>(store);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        return copy;
    }

    /**
     * Sets the storage.
     *
     * Used for testing!
     *
     * @param atomicValues the values to set the storage to
     */
    void set(final Map<String, Tuple2<AtomicType, Object>> atomicValues) {
        final long stamp = lock.writeLock();
        try {
            store.clear();
            store.putAll(atomicValues);
        } finally {
            lock.unlockWrite(stamp);
        }
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
    @Nullable Tuple2<AtomicType, Object> put(final String id, final Tuple2<AtomicType, Object> atomicValue) {
        final long stamp = lock.writeLock();
        try {
            return store.put(id, atomicValue);
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
