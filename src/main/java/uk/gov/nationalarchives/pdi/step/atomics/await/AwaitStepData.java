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
package uk.gov.nationalarchives.pdi.step.atomics.await;

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import uk.gov.nationalarchives.pdi.step.atomics.AtomicStorage;
import uk.gov.nationalarchives.pdi.step.atomics.AtomicType;

import javax.annotation.Nullable;


public class AwaitStepData extends BaseStepData implements StepDataInterface {

    private RowMetaInterface outputRowMeta;
    private String atomicIdFieldName;
    private int atomicIdFieldIndex;
    private RowSet continueOutputRowSet = null;
    private RowSet atomicValueOutputRowSet = null;
    private RowSet timeoutOutputRowSet = null;

    public AwaitStepData() {
        super();
    }

    public @Nullable Object getAtomic(final String id, final AtomicType atomicType) throws IllegalStateException {
        return AtomicStorage.INSTANCE.getAtomic(id, atomicType);
    }

    public Object getOrCreateAtomic(final String id, final AtomicType atomicType, final String initialValue) throws IllegalStateException {
        return AtomicStorage.INSTANCE.getOrCreateAtomic(id, atomicType, initialValue);
    }

    public boolean removeAtomic(final String id) {
        return AtomicStorage.INSTANCE.removeAtomic(id);
    }

    // <editor-fold desc="get/set properties">
    public RowMetaInterface getOutputRowMeta() {
        return outputRowMeta;
    }

    public void setOutputRowMeta(final RowMetaInterface outputRowMeta) {
        this.outputRowMeta = outputRowMeta;
    }

    public String getAtomicIdFieldName() {
        return atomicIdFieldName;
    }

    public void setAtomicIdFieldName(final String atomicIdFieldName) {
        this.atomicIdFieldName = atomicIdFieldName;
    }

    public int getAtomicIdFieldIndex() {
        return atomicIdFieldIndex;
    }

    public void setAtomicIdFieldIndex(final int atomicIdFieldIndex) {
        this.atomicIdFieldIndex = atomicIdFieldIndex;
    }

    public RowSet getContinueOutputRowSet() {
        return continueOutputRowSet;
    }

    public void setContinueOutputRowSet(final RowSet continueOutputRowSet) {
        this.continueOutputRowSet = continueOutputRowSet;
    }

    public RowSet getAtomicValueOutputRowSet() {
        return atomicValueOutputRowSet;
    }

    public void setAtomicValueOutputRowSet(final RowSet atomicValueOutputRowSet) {
        this.atomicValueOutputRowSet = atomicValueOutputRowSet;
    }

    public RowSet getTimeoutOutputRowSet() {
        return timeoutOutputRowSet;
    }

    public void setTimeoutOutputRowSet(final RowSet timeoutOutputRowSet) {
        this.timeoutOutputRowSet = timeoutOutputRowSet;
    }

    // </editor-fold>
}