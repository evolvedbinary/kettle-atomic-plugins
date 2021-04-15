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
package uk.gov.nationalarchives.pdi.step.atomics.compareandset;

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import uk.gov.nationalarchives.pdi.step.atomics.ActionIfNoAtomic;
import uk.gov.nationalarchives.pdi.step.atomics.ActionIfUnableToSet;
import uk.gov.nationalarchives.pdi.step.atomics.AtomicType;
import uk.gov.nationalarchives.pdi.step.atomics.ErrorCodes;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CompareAndSetStep extends BaseStep implements StepInterface {

    private static Class<?> PKG = CompareAndSetStep.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    public CompareAndSetStep(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int copyNr,
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
        final CompareAndSetStepMeta meta = (CompareAndSetStepMeta) smi;
        final CompareAndSetStepData data = (CompareAndSetStepData) sdi;

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
                this.logDebug("CAS No Atomic object for id: {0}, and ActionIfNoAtomic == Continue", atomicId);

                // is there a Continue target step?
                if (meta.getContinueTargetStep() != null) {

                    // send row to the Continue output of the step
                    this.putRowTo(data.getOutputRowMeta(), row, data.getContinueOutputRowSet());

                    this.logDebug("CAS No Atomic, CONTINUE: <{0}>",atomicId);
                    logLineNumber();

                    return true; // row done!

                } else {
                    // raise an exception
                    throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.NoContinueTargetStep"));
                }


            } else if (ActionIfNoAtomic.Error == actionIfNoAtomic) {
                // send row to the error output of the step
                final String errorMessage = "CAS No Atomic object for id: " + atomicId + ", and ActionIfNoAtomic == Error";
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
                        final String errorMessage = "CAS Timeout (" + waitAtomicTimeout + "ms) exceeded whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait";
                        this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), ErrorCodes.NO_SUCH_ATOMIC_WAIT_TIMEOUT.getCode());
                        logLineNumber();
                        return true; // row done!
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt(); // restore interrupted flag
                    // send row to the error output of the step
                    final String errorMessage = "CAS Thread interrupted whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait";
                    this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), ErrorCodes.NO_SUCH_ATOMIC_WAIT_INTERRUPTED.getCode());
                    logLineNumber();
                    return true; // row done!
                }
            }
        }  // end while

        // At this point we have an atomicObj
        final ActionIfUnableToSet actionIfUnableToSet = meta.getActionIfUnableToSet();
        CompareAndSetTarget casTarget = null;

        final List<CompareAndSetTarget> compareAndSetValues = meta.getCompareAndSetValues();
        if (compareAndSetValues != null && !compareAndSetValues.isEmpty()) {
            final long unableToSetLoopCheckPeriod = meta.getUnableToSetLoopCheckPeriod();
            final long unableToSetTimeoutPeriod = meta.getUnableToSetLoopTimeout();

            boolean set = false;
            long waitedForSet = 0;
            while (true) {

                set = false;

                // try and set each value in turn
                for (final CompareAndSetTarget compareAndSetValue : compareAndSetValues) {

                    if (AtomicType.Boolean == atomicType) {
                        final AtomicBoolean atomicBoolean = (AtomicBoolean) atomicObj;
                        final boolean compareValue = Boolean.valueOf(compareAndSetValue.getCompareValue());
                        final boolean setValue = Boolean.valueOf(compareAndSetValue.getSetValue());
                        set = atomicBoolean.compareAndSet(compareValue, setValue);

                    } else if (AtomicType.Integer == atomicType) {
                        final AtomicInteger atomicInteger = (AtomicInteger) atomicObj;
                        final int compareValue = Integer.valueOf(compareAndSetValue.getCompareValue());
                        final int setValue = Integer.valueOf(compareAndSetValue.getSetValue());
                        set = atomicInteger.compareAndSet(compareValue, setValue);

                    } else {
                        throw new IllegalArgumentException("Unknown AtomicType: " + atomicType.name());
                    }

                    if (set) {
                        casTarget = compareAndSetValue;
                        break; // CaS succeeded, exit for loop
                    }
                }  // end for

                if (set) {
                    break; // CaS succeeded, exit while loop
                }

                if (ActionIfUnableToSet.Skip == actionIfUnableToSet) {

                    // send row to the 'Skip' output of the step
//                        this.logDebug("CAS Unable to set: <{0}>, and ActionIfUnableToSet == Skip", atomicId);

                    // is there a Skip target step?
                    if (meta.getSkipTargetStep() != null) {

                        // send row to the Skip output of the step
                        this.putRowTo(data.getOutputRowMeta(), row, data.getSkipOutputRowSet());

                        this.logDebug("CAS SKIP: <{0}>",atomicId);
                        logLineNumber();

                        return true; // row done!

                    } else {
                        // raise an exception
                        throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.NoSkipTargetStep"));
                    }


                } else if (ActionIfUnableToSet.Error == actionIfUnableToSet) {
                    // TODO(AR) should this row be directed to error of step, i.e. putError(...)
                    throw new KettleException("Unable to Compare And Set Value for: " + atomicId);

                } else if (ActionIfUnableToSet.Loop == actionIfUnableToSet) {

                    // wait before loop to reattempt CaS
                    try {
                        Thread.sleep(unableToSetLoopCheckPeriod);
                        waitedForSet += unableToSetLoopCheckPeriod;
                        if (unableToSetTimeoutPeriod != -1 && waitedForSet > unableToSetTimeoutPeriod) {
                            // TIMEOUT reached!

                            // is there a timeout target step?
                            if (meta.getTimeoutTargetStep() != null) {

                                // send row to the timeout output of the step
                                this.putRowTo(data.getOutputRowMeta(), row, data.getTimeoutOutputRowSet());

                                this.logDebug("CAS TIMEOUT: <{0}>{1}", atomicId, casTarget.toString());
                                logLineNumber();

                                return true; // row done!

                            } else {
                                // raise an exception
                                throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.NoTimeoutTargetStep"));
                            }
                        }
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt(); // restore interrupted flag
                        // send row to the error output of the step
                        final String errorMessage = "Thread interrupted whilst waiting to CAS Atomic value for id: " + atomicId;
                        this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), ErrorCodes.CAS_ATOMIC_WAIT_INTERRUPTED.getCode());
                        logLineNumber();
                        return true; // row done!
                    }

                } else {
                    throw new IllegalArgumentException("CAS Unknown ActionIfUnableToSet: " + actionIfUnableToSet.name());
                }
            }
        }

        //TODO(AR) how is an Atomic ever destroyed, we want to shrink the Map when we are finished with the things

        if (casTarget != null) {
            // send to specific target for CAS success
            final Set<RowSet> casTargetRowSets = data.getCasOutputRowSets().get(casTarget.getCompareValue());
            if (casTargetRowSets == null || casTargetRowSets.isEmpty()) {
                throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindTargetRowSetForStep", new Object[] { casTarget.getTargetStep() }));
            }

            for (final RowSet casTargetRowSet : casTargetRowSets) {
                this.putRowTo(data.getOutputRowMeta(), row, casTargetRowSet);
            }

            this.logDebug("CAS OK: <{0}>{1}", atomicId, casTarget.toString());
            logLineNumber();

            return true;  // row done!

        } else {
            throw new KettleException(new IllegalStateException("CAS It should not be possible to reach this state!"));
        }
    }

    private void logLineNumber() {
        if (checkFeedback(getLinesRead())) {
            if (log.isBasic()) {
                logBasic(BaseMessages.getString(PKG, "CompareAndSetStep.Log.LineNumber") + getLinesRead());
            }
        }
    }

    @Override
    public boolean init(final StepMetaInterface smi, final StepDataInterface sdi) {
        final CompareAndSetStepMeta meta = (CompareAndSetStepMeta) smi;

        if (!super.init(smi, sdi)) {
            return false;
        }

        if (Utils.isEmpty(meta.getAtomicIdFieldName())) {
            logError(BaseMessages.getString(PKG, "CompareAndSetStep.Log.NoAtomicIDFieldSpecified"));
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
    void createOutputValueMapping(final CompareAndSetStepMeta meta, final CompareAndSetStepData data) throws KettleException {
        final RowMetaInterface outputRowMeta = getInputRowMeta().clone();
        meta.getFields(outputRowMeta, getStepname(), null, null, this, repository, metaStore);
        data.setOutputRowMeta(outputRowMeta);

        final String atomicIdFieldName = environmentSubstitute(meta.getAtomicIdFieldName());
        data.setAtomicIdFieldName(atomicIdFieldName);
        data.setAtomicIdFieldIndex(getInputRowMeta().indexOfValue(atomicIdFieldName));
        if (data.getAtomicIdFieldIndex() < 0) {
            throw new KettleException(BaseMessages.getString( PKG, "CompareAndSetStep.Exception.UnableToFindFieldName", atomicIdFieldName));
        }

        try {
            final StepIOMetaInterface ioMeta = meta.getStepIOMeta();

            // There is one or many CAS target for each target stream.
            // The ioMeta object also has optional target streams for: continue, skip, and timeout.

            final List<StreamInterface> targetStreams = ioMeta.getTargetStreams();
            for (int i = 0; i < targetStreams.size(); i++) {
                final CompareAndSetTarget compareAndSetValue = (CompareAndSetTarget) targetStreams.get(i).getSubject();
                if (compareAndSetValue == null) {
                    break; // Skip over default option
                }

                if (compareAndSetValue.getTargetStep() == null) {
                    throw new KettleException(BaseMessages.getString(
                            PKG, "CompareAndSetStep.Log.NoTargetStepSpecifiedForValue", compareAndSetValue.getCompareValue(), compareAndSetValue.getSetValue()));
                }

                final RowSet rowSet = findOutputRowSet(compareAndSetValue.getTargetStep().getName());
                if (rowSet == null) {
                    throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindTargetRowSetForStep", new Object[] { compareAndSetValue.getTargetStep() }));
                }

                // store the compare value and the rowset
                data.getCasOutputRowSets().put(compareAndSetValue.getCompareValue(), rowSet);
            }

            if (meta.getContinueTargetStep() != null) {
                final RowSet rowSet = findOutputRowSet(meta.getContinueTargetStep().getName());
                if (rowSet != null) {
                    data.setContinueOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindContinueTargetRowSetForStep", new Object[]{ meta.getContinueTargetStep() }));
                }
            }

            if (meta.getSkipTargetStep() != null) {
                final RowSet rowSet = findOutputRowSet(meta.getSkipTargetStep().getName());
                if (rowSet != null) {
                    data.setSkipOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindSkipTargetRowSetForStep", new Object[]{ meta.getSkipTargetStep() }));
                }
            }

            if (meta.getTimeoutTargetStep() != null) {
                final RowSet rowSet = findOutputRowSet(meta.getTimeoutTargetStep().getName());
                if (rowSet != null) {
                    data.setTimeoutOutputRowSet(rowSet);
                } else {
                    throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindTimeoutTargetRowSetForStep", new Object[] { meta.getTimeoutTargetStep() }));
                }
            }
        } catch (final Exception e) {
            throw new KettleException(e);
        }
    }
}
