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

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import javax.annotation.Nullable;

public abstract class AbstractAtomicStepMeta extends BaseStepMeta implements StepMetaInterface {

    // <editor-fold desc="settings XML element names">
    protected static final String ELEM_NAME_ATOMIC_ID_FIELD_NAME = "atomicIdFieldName";
    protected static final String ELEM_NAME_ATOMIC_TYPE = "atomicType";
    protected static final String ELEM_NAME_ACTION_IF_NO_ATOMIC = "actionIfNoAtomic";
    protected static final String ATTR_NAME_CONTINUE_TARGET_STEP = "continueTargetStep";
    protected static final String ATTR_NAME_VALUE = "value";
    protected static final String ELEM_NAME_ATOMIC_VALUES = "atomicValues";
    protected static final String ELEM_NAME_ATOMIC_VALUE = "atomicValue";
    protected static final String ATTR_NAME_TARGET_STEP = "targetStep";
    protected static final String ATTR_NAME_CHECK_PERIOD = "checkPeriod";
    protected static final String ATTR_NAME_TIMEOUT = "timeout";
    protected static final String ATTR_NAME_TIMEOUT_TARGET_STEP = "timeoutTargetStep";
    // </editor-fold>

    protected static final long DEFAULT_CHECK_PERIOD = 100; // ms
    protected static final long TIMEOUT_DISABLED = -1; // No timeout
    protected static final long DEFAULT_TIMEOUT = TIMEOUT_DISABLED;

    // <editor-fold desc="settings">
    protected String atomicIdFieldName;
    protected AtomicType atomicType;
    protected ActionIfNoAtomic actionIfNoAtomic;
    protected String continueTargetStepname;
    @Nullable protected String initialiseAtomicValue;
    protected long waitAtomicCheckPeriod = DEFAULT_CHECK_PERIOD;
    protected long waitAtomicTimeout = DEFAULT_TIMEOUT;
    protected String timeoutTargetStepname;
    // </editor-fold>

    @Nullable protected StepMeta continueTargetStep;
    @Nullable protected StepMeta timeoutTargetStep;

    @Override
    public void setDefault() {
        atomicIdFieldName = "";
        atomicType = AtomicType.Boolean;
        actionIfNoAtomic = ActionIfNoAtomic.Continue;
        continueTargetStepname = null;
        initialiseAtomicValue = null;
        waitAtomicCheckPeriod = DEFAULT_CHECK_PERIOD;
        waitAtomicTimeout = DEFAULT_TIMEOUT;
        timeoutTargetStepname = null;
    }

    // <editor-fold desc="settings getters and setters">
    public String getAtomicIdFieldName() {
        return atomicIdFieldName;
    }

    public void setAtomicIdFieldName(final String atomicIdFieldName) {
        this.atomicIdFieldName = atomicIdFieldName;
    }

    public AtomicType getAtomicType() {
        return atomicType;
    }

    public void setAtomicType(final AtomicType atomicType) {
        this.atomicType = atomicType;
    }

    public ActionIfNoAtomic getActionIfNoAtomic() {
        return actionIfNoAtomic;
    }

    public void setActionIfNoAtomic(final ActionIfNoAtomic actionIfNoAtomic) {
        this.actionIfNoAtomic = actionIfNoAtomic;
    }

    public String getContinueTargetStepname() {
        return continueTargetStepname;
    }

    public void setContinueTargetStepname(final String continueTargetStepname) {
        this.continueTargetStepname = continueTargetStepname;
    }

    @Nullable public StepMeta getContinueTargetStep() {
        return continueTargetStep;
    }

    public void setContinueTargetStep(@Nullable final StepMeta continueTargetStep) {
        this.continueTargetStep = continueTargetStep;
    }

    public @Nullable String getInitialiseAtomicValue() {
        return initialiseAtomicValue;
    }

    public void setInitialiseAtomicValue(@Nullable final String initialiseAtomicValue) {
        this.initialiseAtomicValue = initialiseAtomicValue;
    }

    public long getWaitAtomicCheckPeriod() {
        return waitAtomicCheckPeriod;
    }

    public void setWaitAtomicCheckPeriod(final long waitAtomicCheckPeriod) {
        this.waitAtomicCheckPeriod = waitAtomicCheckPeriod;
    }

    public long getWaitAtomicTimeout() {
        return waitAtomicTimeout;
    }

    public void setWaitAtomicTimeout(final long waitAtomicTimeout) {
        this.waitAtomicTimeout = waitAtomicTimeout;
    }

    public String getTimeoutTargetStepname() {
        return timeoutTargetStepname;
    }

    public void setTimeoutTargetStepname(final String timeoutTargetStepname) {
        this.timeoutTargetStepname = timeoutTargetStepname;
    }

    public StepMeta getTimeoutTargetStep() {
        return timeoutTargetStep;
    }

    public void setTimeoutTargetStep(final StepMeta timeoutTargetStep) {
        this.timeoutTargetStep = timeoutTargetStep;
    }

    // </editor-fold>

}
