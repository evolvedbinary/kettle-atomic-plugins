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
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import uk.gov.nationalarchives.pdi.step.atomics.*;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

import static uk.gov.nationalarchives.pdi.step.atomics.Util.*;

public class AwaitStep extends BaseStep implements StepInterface {

    static final String IGNORE_STEPNAME_FOR_TEST = "__IGNORE_STEPNAME_FOR_TEST__";

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
        AtomicValue atomicValue = null;
        while (true) {
            if (ActionIfNoAtomic.Initialise == actionIfNoAtomic) {
                atomicValue = data.getOrCreateAtomic(atomicId, atomicType, meta.getInitialiseAtomicValue());
            } else {
                atomicValue = data.getAtomic(atomicId, atomicType);
            }

            if (atomicValue != null) {
                break;  // exit while loop
            }


            // when atomicValue null...

            if (ActionIfNoAtomic.Continue == actionIfNoAtomic) {
                // send row to the 'Continue' output of the step
                this.logDebug("Await No Atomic object for id: {0}, and ActionIfNoAtomic == Continue", atomicId);

                // is there a Continue target step?
                final String metaContinueTargetStepName = meta.getContinueTargetStep() != null ? meta.getContinueTargetStep().getName() : meta.getContinueTargetStepname();
                if (isNotEmpty(metaContinueTargetStepName)) {

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

        // At this point we have an atomicValue
        AwaitTarget awaitTarget = null;
        final List<AwaitTarget> awaitValues = meta.getAwaitValues();
        if (awaitValues != null && !awaitValues.isEmpty()) {
            final long waitLoopCheckPeriod = meta.getWaitLoopCheckPeriod();
            final long waitLoopTimeout = meta.getWaitLoopTimeout();

            // check if one of the options is to await `null`
            AwaitTarget awaitNullTarget = null;
            for (final AwaitTarget awaitValue : awaitValues) {
                if (awaitValue.getAtomicValue() == null) {
                    awaitNullTarget = awaitValue;
                    break;
                }
            }

            boolean atomicIsEqual = false;
            long waited = 0;
            while (true) {

                if (awaitNullTarget != null && atomicValue == null) {
                    // null is a valid value to check for, i.e. already discarded
                    atomicIsEqual = true;
                    awaitTarget = awaitNullTarget;

                } else {
                    atomicIsEqual = false;

                    for (final AwaitTarget awaitValue : awaitValues) {
                        @Nullable final String awaitAtomicValue = awaitValue.getAtomicValue();

                        if (awaitAtomicValue == null) {
                            // handled above in awaitNullTarget logic
                            continue;

                        } else if (AtomicType.Boolean == atomicType) {
                            final AtomicBooleanValue atomicBoolean = (AtomicBooleanValue) atomicValue;
                            final boolean awaitValueBoolean = Boolean.parseBoolean(awaitAtomicValue);
                            atomicIsEqual = awaitValueBoolean == atomicBoolean.get();

                        } else if (AtomicType.Integer == atomicType) {
                            final int awaitValueInt = Integer.parseInt(awaitAtomicValue);
                            final AtomicIntegerValue atomicInteger = (AtomicIntegerValue) atomicValue;
                            atomicIsEqual = awaitValueInt == atomicInteger.get();

                        } else {
                            throw new IllegalArgumentException("Unknown AtomicType: " + atomicType.name());
                        }

                        if (atomicIsEqual) {
                            awaitTarget = awaitValue;
                            break; // atomic value is equal to expected, exit for loop
                        }

                    }  // end for
                }


                if (atomicIsEqual) {
                    break; // atomic value is equal to expected, exit while loop
                }

                // wait and check again
                try {
                    Thread.sleep(waitLoopCheckPeriod);
                    waited += waitLoopCheckPeriod;
                    if (waitLoopTimeout != -1 && waited > waitLoopTimeout) {
                        // TIMEOUT reached!

                        // is there a timeout target step?
                        final String metaTimeoutTargetStepName = meta.getTimeoutTargetStep() != null ? meta.getTimeoutTargetStep().getName() : meta.getTimeoutTargetStepname();
                        if (isNotEmpty(metaTimeoutTargetStepName)) {

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

                // refresh the atomic object
                atomicValue = data.getAtomic(atomicId, atomicType);

            }  // end while
        }

        if (awaitTarget != null) {
            // send to specific target for Await success
            final Set<RowSet> atomicValueTargetRowSets = data.getAtomicValueOutputRowSets().get(awaitTarget.getAtomicValue());
            if (atomicValueTargetRowSets == null || atomicValueTargetRowSets.isEmpty()) {
                throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.UnableToFindTargetRowSetForStep", new Object[] { awaitTarget.getTargetStep() != null ? awaitTarget.getTargetStep().getName() : awaitTarget.getTargetStepname() }));
            }

            for (final RowSet awaitValueTargetRowSet : atomicValueTargetRowSets) {
                this.putRowTo(data.getOutputRowMeta(), row, awaitValueTargetRowSet);
            }

            this.logDebug("Await DONE: <{0}>[{1}]", atomicId, strNullIfNull(nullIfEmpty(awaitTarget.getAtomicValue())));

            if (awaitTarget.isDiscardAtomic()) {
                // discard the atomic if requested to do so
                if (!data.removeAtomic(atomicId)) {
                    this.logError("Unable to discard Atomic with ID: {0}", atomicId);
                }
            }


        } else {
            //send to default output if no Await Target
            this.putRow(data.getOutputRowMeta(), row);
        }

        logLineNumber();

        return true;  // row done!
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
     * <li>for every discovered output rowset put it as a key-value: 'await value'-'output rowSet'.
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

//        try {

            final StepIOMetaInterface ioMeta = meta.getStepIOMeta();

            // There is one or many CAS target for each target stream.
            final List<StreamInterface> targetStreams = ioMeta.getTargetStreams();
            for (int i = 0; i < targetStreams.size(); i++) {
                final Object subject = targetStreams.get(i).getSubject();
                if (subject == null) {
                    continue;  // Skip over default option
                }
                if (!(subject instanceof AwaitTarget)) {
                    continue;  // Skip over other target type
                }

                final AwaitTarget awaitValue = (AwaitTarget) subject;

                final String awaitTargetStepName = awaitValue.getTargetStep() != null ? awaitValue.getTargetStep().getName() : awaitValue.getTargetStepname();
                if (isNullOrEmpty(awaitTargetStepName)) {
                    throw new KettleException(BaseMessages.getString(
                            PKG, "AwaitStep.Log.NoTargetStepSpecifiedForValue", strNullIfNull(nullIfEmpty(awaitValue.getAtomicValue()))));
                }

                if (!IGNORE_STEPNAME_FOR_TEST.equals(awaitTargetStepName)) {
                    final RowSet rowSet = findOutputRowSet(awaitTargetStepName);
                    if (rowSet == null) {
                        throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.UnableToFindAtomicValueTargetRowSetForStep", new Object[]{awaitTargetStepName}));
                    }

                    // store the await value and the rowset
                    data.getAtomicValueOutputRowSets().put(awaitValue.getAtomicValue(), rowSet);
                }
            }


            // The ioMeta object also has optional target streams for: continue, and timeout.

            final String metaContinueTargetStepName = meta.getContinueTargetStep() != null ? meta.getContinueTargetStep().getName() : meta.getContinueTargetStepname();
            if (isNotEmpty(metaContinueTargetStepName)) {
                final RowSet rowSet = findOutputRowSet(metaContinueTargetStepName);
                if (rowSet != null) {
                    data.setContinueOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.UnableToFindContinueTargetRowSetForStep", new Object[]{ metaContinueTargetStepName }));
                }
            }

            final String metaTimeoutTargetStepName = meta.getTimeoutTargetStep() != null ? meta.getTimeoutTargetStep().getName() : meta.getTimeoutTargetStepname();
            if (isNotEmpty(metaTimeoutTargetStepName)) {
                final RowSet rowSet = findOutputRowSet(metaTimeoutTargetStepName);
                if (rowSet != null) {
                    data.setTimeoutOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.UnableToFindTimeoutTargetRowSetForStep", new Object[] { metaTimeoutTargetStepName }));
                }
            }
//        } catch (final Exception e) {
//            throw new KettleException(e);
//        }
    }
}
