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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import uk.gov.nationalarchives.pdi.step.atomics.ActionIfNoAtomic;
import uk.gov.nationalarchives.pdi.step.atomics.AtomicType;
import uk.gov.nationalarchives.pdi.step.atomics.ErrorCodes;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AwaitStep extends BaseStep implements StepInterface {

    private static Class<?> PKG = AwaitStep.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    public AwaitStep(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int copyNr,
            final TransMeta transMeta, final Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException {

        Object[] row = getRow(); // try and get a row
        if (row == null) {
            // no more rows...
            setOutputDone();
            return false;  // signal that we are DONE

        }

        // process a row...
        final AwaitStepMeta meta = (AwaitStepMeta) smi;
        final AwaitStepData data = (AwaitStepData) sdi;

        if (first) {
            first = false;

            // map input to output streams
            createOutputValueMapping(meta, data);
        }

        final Object atomicIdFieldNameValue = row[data.getAtomicIdFieldIndex()];

        final String atomicId;
        if (atomicIdFieldNameValue instanceof String) {
            atomicId = (String) atomicIdFieldNameValue;
        } else {
            throw new KettleException("Expected field " + data.getAtomicIdFieldName() + " to contain a String, but found "
                    + atomicIdFieldNameValue.getClass());
        }

        final ActionIfNoAtomic actionIfNoAtomic = meta.getActionIfNoAtomic();
        final AtomicType atomicType = meta.getAtomicType();
        final long waitAtomicCheckPeriod = meta.getWaitAtomicCheckPeriod();
        final long waitAtomicTimeout = meta.getWaitAtomicTimeout();


        long waitedForAtomic = 0;
        Object atomicObj = null;
        while (true) {
            if (ActionIfNoAtomic.Initialise == actionIfNoAtomic) {
                atomicObj = data.getOrCreateAtomic(atomicId, atomicType, meta.getInitialiseAtomicValue());
            } else {
                atomicObj = data.getAtomic(atomicId, atomicType);
            }

            if (atomicObj != null) {
                break;  // exit while loop
            }


            // when atomicObj null...

            if (ActionIfNoAtomic.Continue == actionIfNoAtomic) {
                // send row to the 'Continue' output of the step
                this.logDebug("Await No Atomic object for id: {0}, and ActionIfNoAtomic == Continue", atomicId);

                // is there a Continue target step?
                if (meta.getContinueTargetStep() != null) {

                    // send row to the Continue output of the step
                    this.putRowTo(data.getOutputRowMeta(), row, data.getContinueOutputRowSet());

                    this.logDebug("Await No Atomic, CONTINUE: <{0}>", atomicId);
                    logLineNumber();

                    return true; // row done!

                } else {
                    // raise an exception
                    throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.NoContinueTargetStep"));
                }

            } else if (ActionIfNoAtomic.Error == actionIfNoAtomic) {
                // send row to the error output of the step
                final String errorMessage = "No Atomic object for id: " + atomicId + ", and ActionIfNoAtomic == Error";
                this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), ErrorCodes.NO_SUCH_ATOMIC.getCode());
                logLineNumber();
                return true; // row done!

            } else if (ActionIfNoAtomic.Wait == actionIfNoAtomic) {
                try {
                    Thread.sleep(waitAtomicCheckPeriod);
                    waitedForAtomic += waitAtomicCheckPeriod;
                    if (waitAtomicTimeout != -1 && waitedForAtomic > waitAtomicTimeout) {
                        // TIMEOUT reached!
                        // send row to the error output of the step
                        final String errorMessage = "Timeout (" + waitAtomicTimeout + "ms) exceeded whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait";
                        this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), ErrorCodes.NO_SUCH_ATOMIC_WAIT_TIMEOUT.getCode());
                        logLineNumber();
                        return true; // row done!
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt(); // restore interrupted flag
                    // send row to the error output of the step
                    final String errorMessage = "Thread interrupted whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait";
                    this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), ErrorCodes.NO_SUCH_ATOMIC_WAIT_INTERRUPTED.getCode());
                    logLineNumber();
                    return true; // row done!
                }
            }
        }  // end while

        // At this point we have an atomicObj
        boolean atomicIsEqual = false;

        final long waitLoopCheckPeriod = meta.getWaitLoopCheckPeriod();
        final long waitLoopTimeout = meta.getWaitLoopTimeout();

        long waited = 0;
        while (true) {

            if (AtomicType.Boolean == atomicType) {
                final AtomicBoolean atomicBoolean = (AtomicBoolean) atomicObj;
                final boolean awaitValueBoolean = Boolean.valueOf(meta.getAtomicValue());
                atomicIsEqual = awaitValueBoolean == atomicBoolean.get();

            } else if (AtomicType.Integer == atomicType) {
                final int awaitValueInt = Integer.valueOf(meta.getAtomicValue());
                final AtomicInteger atomicInteger = (AtomicInteger) atomicObj;
                atomicIsEqual = awaitValueInt == atomicInteger.get();

            } else {
                throw new IllegalArgumentException("Unknown AtomicType: " + atomicType.name());
            }

            if (atomicIsEqual) {
                break; // atomic value is equal to expected, exit for loop
            }

            // wait and check again
            try {
                Thread.sleep(waitLoopCheckPeriod);
                waited += waitLoopCheckPeriod;
                if (waitLoopTimeout != -1 && waited > waitLoopTimeout) {
                    // TIMEOUT reached!

                    // is there a timeout target step?
                    if (meta.getTimeoutTargetStep() != null) {

                        // send row to the timeout output of the step
                        this.putRowTo(data.getOutputRowMeta(), row, data.getTimeoutOutputRowSet());
                        logLineNumber();

                        return true; // row done!

                    } else {
                        // raise an exception
                        throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.NoTimeoutTargetStep"));
                    }
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt(); // restore interrupted flag
                // send row to the error output of the step
                final String errorMessage = "Thread interrupted whilst waiting for Atomic value for id: " + atomicId;
                this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), ErrorCodes.AWAIT_ATOMIC_WAIT_INTERRUPTED.getCode());
                logLineNumber();
                return true; // row done!
            }
        }  // end while

        // send to specific target for Await success
        final StepMeta atomicValueTargetStep = meta.getAtomicValueTargetStep();
        if (atomicValueTargetStep != null) {

            this.putRowTo(data.getOutputRowMeta(), row, data.getAtomicValueOutputRowSet());

            this.logDebug("Await DONE: <{0}>[{1}]", atomicId, meta.getAtomicValue());
            logLineNumber();

            if (meta.isDiscardAtomic()) {
                // discard the atomic if requested to do so
                if (!data.removeAtomic(atomicId)) {
                    this.logError("Unable to discard Atomic with ID: {0}", atomicId);
                }
            }

            return true;  // row done!
        } else {
            // raise an exception
            throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.NoAtomicValueTargetStep"));
        }
    }

    private void logLineNumber() {
        if (checkFeedback(getLinesRead())) {
            if (log.isBasic()) {
                logBasic(BaseMessages.getString(PKG, "AwaitStep.Log.LineNumber") + getLinesRead());
            }
        }
    }

    @Override
    public boolean init(final StepMetaInterface smi, final StepDataInterface sdi) {
        final AwaitStepMeta meta = (AwaitStepMeta) smi;

        if (!super.init(smi, sdi)) {
            return false;
        }

        if (Utils.isEmpty(meta.getAtomicIdFieldName())) {
            logError(BaseMessages.getString(PKG, "AwaitStep.Log.NoAtomicIDFieldSpecified"));
            return false;
        }

        return true;
    }

    /**
     * This will prepare step for execution:
     * <ol>
     * <li>will copy input row meta info, fields info, etc. step related info
     * <li>will get step IO meta info and discover target streams for target output steps
     * <li>for every target output find output rowset and expected value.
     * <li>for every discovered output rowset put it as a key-value: 'compare value'-'output rowSet'.
     * </ol>
     *
     * @throws KettleException
     *           if something goes wrong during step preparation.
     */
    void createOutputValueMapping(final AwaitStepMeta meta, final AwaitStepData data) throws KettleException {
        final RowMetaInterface outputRowMeta = getInputRowMeta().clone();
        meta.getFields(outputRowMeta, getStepname(), null, null, this, repository, metaStore);
        data.setOutputRowMeta(outputRowMeta);

        final String atomicIdFieldName = environmentSubstitute(meta.getAtomicIdFieldName());
        data.setAtomicIdFieldName(atomicIdFieldName);
        data.setAtomicIdFieldIndex(getInputRowMeta().indexOfValue(atomicIdFieldName));
        if (data.getAtomicIdFieldIndex() < 0) {
            throw new KettleException(BaseMessages.getString( PKG, "AwaitStep.Exception.UnableToFindFieldName", atomicIdFieldName));
        }

        try {

            // The ioMeta object also has optional target streams for: continue, skip, and timeout.

            if (meta.getContinueTargetStep() != null) {
                final RowSet rowSet = findOutputRowSet(meta.getContinueTargetStep().getName());
                if (rowSet != null) {
                    data.setContinueOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.UnableToFindContinueTargetRowSetForStep", new Object[]{ meta.getContinueTargetStep() }));
                }
            }

            if (meta.getAtomicValueTargetStep() != null) {
                final RowSet rowSet = findOutputRowSet(meta.getAtomicValueTargetStep().getName());
                if (rowSet != null) {
                    data.setAtomicValueOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindAtomicValueTargetRowSetForStep", new Object[]{ meta.getAtomicValueTargetStep() }));
                }
            }

            if (meta.getTimeoutTargetStep() != null) {
                final RowSet rowSet = findOutputRowSet(meta.getTimeoutTargetStep().getName());
                if (rowSet != null) {
                    data.setTimeoutOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.UnableToFindTimeoutTargetRowSetForStep", new Object[] { meta.getTimeoutTargetStep() }));
                }
            }
        } catch (final Exception e) {
            throw new KettleException(e);
        }
    }
}
