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

import com.evolvedbinary.j8fu.Either;
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

import java.util.List;
import java.util.Set;

import static com.evolvedbinary.j8fu.Either.Left;
import static com.evolvedbinary.j8fu.Either.Right;
import static uk.gov.nationalarchives.pdi.step.atomics.Util.isNotEmpty;
import static uk.gov.nationalarchives.pdi.step.atomics.Util.isNullOrEmpty;

public class CompareAndSetStep extends AbstractAtomicStep {

    private enum CASAtomicRouteTarget {
        DEFAULT,
        SKIP,
        ERROR,
        TIMEOUT,
        THREAD_INTERRUPTED
    }

    private static Class<?> PKG = CompareAndSetStep.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    public CompareAndSetStep(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int copyNr,
                             final TransMeta transMeta, final Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException {
        final Object[] row = getRow(); // try and get a row
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

        final String atomicId = getAtomicId(data, row);

        // 1. get (or initialise) the AtomicValue
        final Either<GetAtomicRouteTarget, AtomicValue> routeOrAtomic = getAtomic(meta, data, atomicId);
        if (routeOrAtomic.isLeft()) {
            // could not get (or initialise) AtomicValue, so route row to specific output target...
            final GetAtomicRouteTarget route = routeOrAtomic.left().get();
            switch (route) {
                case CONTINUE:
                    putRowToContinueTarget(meta, data, atomicId, row, BaseMessages.getString(PKG, "CompareAndSetStep.Log.NoContinueTargetStep"));
                    return true;

                case ERROR:
                    putRowToErrorTarget(data, row, ErrorCode.NO_SUCH_ATOMIC, "CAS No Atomic object for id: " + atomicId + ", and ActionIfNoAtomic == Error");
                    return true;

                case TIMEOUT:
                    // NOTE: this is intentionally sent to the error target at this stage, the timeout target is reserved for the await value part further below
                    putRowToErrorTarget(data, row, ErrorCode.NO_SUCH_ATOMIC_WAIT_TIMEOUT, "CAS Timeout (" + meta.getWaitAtomicTimeout() + "ms) exceeded whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait");
                    return true;

                case THREAD_INTERRUPTED:
                    putRowToErrorTarget(data, row, ErrorCode.NO_SUCH_ATOMIC_WAIT_INTERRUPTED, "CAS Thread interrupted whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait");
                    return true;
            }
        }

        // At this point we have an AtomicValue
        final AtomicValue atomicValue = routeOrAtomic.right().get();

        // 2. Check/Wait until the AtomicValue reaches one of the await values, and then get the target
        final Either<CASAtomicRouteTarget, CompareAndSetTarget> routeOrCasTarget = casAndGetTarget(meta, data, atomicId, atomicValue);
        if (routeOrCasTarget.isLeft()) {
            // AtomicValue never completed CAS, so route row to specific failure output target...
            final CASAtomicRouteTarget route = routeOrCasTarget.left().get();
            switch (route) {
                case DEFAULT:
                    putRowToDefaultTarget(data, row);
                    return true;

                case SKIP:
                    putRowToSkipTarget(meta, data, atomicId, row);
                    return true;

                case ERROR:
                    putRowToErrorTarget(data, row, ErrorCode.CAS_FAILED, "Unable to Compare And Set Value for: " + atomicId + ", and ActionIfUnableToSet == Error");
                    return true;

                case TIMEOUT:
                    putRowToTimeoutTarget(meta, data, row, BaseMessages.getString(PKG, "CompareAndSetStep.Log.NoTimeoutTargetStep"));
                    return true;

                case THREAD_INTERRUPTED:
                    putRowToErrorTarget(data, row, ErrorCode.CAS_ATOMIC_WAIT_INTERRUPTED, "Thread interrupted whilst waiting to CAS Atomic value for id: " + atomicId);
                    return true;
            }
        }

        // At this point we have a CompareAndSetTarget, i.e. the AtomicValue completed CAS
        final CompareAndSetTarget casTarget = routeOrCasTarget.right().get();

        // We now send the input row to specific targets for CAS success
        final Set<RowSet> casTargetRowSets = data.getOutputRowSets().get(casTarget.getCompareValue());
        if (casTargetRowSets == null || casTargetRowSets.isEmpty()) {
            throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindTargetRowSetForStep", new Object[] { casTarget.getTargetStep() != null ? casTarget.getTargetStep().getName() : casTarget.getTargetStepname() }));
        }

        // send the row to the success targets
        for (final RowSet casTargetRowSet : casTargetRowSets) {
            this.putRowTo(data.getOutputRowMeta(), row, casTargetRowSet);
        }

        this.logDebug("CAS OK: <{0}>{1}", atomicId, casTarget.toString());

        logLineNumber();

        return true;  // row done!
    }

    /**
     * Attempts to CAS the AtomicValue.
     *
     * This method internally will loop approximately every {@link CompareAndSetStepMeta#getUnableToSetLoopCheckPeriod()} ()}
     * until the AtomicValue matches one of the await values, or {@link CompareAndSetStepMeta#getUnableToSetLoopTimeout()} is reached.
     *
     * @param meta the ComapreAndSet Step Meta instance
     * @param data the ComapreAndSet Step Data instance
     * @param atomicId the id of the AtomicValue
     * @param atomicValue the AtomicValue on which we try to CAS
     *
     * @return Either a route to target if the AtomicValue cannot be CAS'd,
     *    or the CompareAndSetTarget to route the output to when it has been CAS'd.
     */
    private Either<CASAtomicRouteTarget, CompareAndSetTarget> casAndGetTarget(final CompareAndSetStepMeta meta, final CompareAndSetStepData data, final String atomicId, AtomicValue atomicValue) {
        final AtomicType atomicType = meta.getAtomicType();
        final ActionIfUnableToSet actionIfUnableToSet = meta.getActionIfUnableToSet();
        final List<CompareAndSetTarget> compareAndSetValues = meta.getCompareAndSetValues();
        if (compareAndSetValues != null && !compareAndSetValues.isEmpty()) {

            final long unableToSetLoopCheckPeriod = meta.getUnableToSetLoopCheckPeriod();
            final long unableToSetTimeout = meta.getUnableToSetLoopTimeout();

            long waited = 0;
            while (true) {

                // try and set each value in turn
                for (final CompareAndSetTarget compareAndSetValue : compareAndSetValues) {

                    if (AtomicType.Boolean == atomicType) {
                        final AtomicBooleanValue atomicBoolean = (AtomicBooleanValue) atomicValue;
                        final boolean compareValue = Boolean.valueOf(compareAndSetValue.getCompareValue());
                        final boolean setValue = Boolean.valueOf(compareAndSetValue.getSetValue());
                        if (atomicBoolean.compareAndSet(compareValue, setValue)) {
                            return Right(compareAndSetValue);
                        }

                    } else if (AtomicType.Integer == atomicType) {
                        final AtomicIntegerValue atomicInteger = (AtomicIntegerValue) atomicValue;
                        final int compareValue = Integer.valueOf(compareAndSetValue.getCompareValue());
                        final int setValue = Integer.valueOf(compareAndSetValue.getSetValue());
                        if (atomicInteger.compareAndSet(compareValue, setValue)) {
                            return Right(compareAndSetValue);
                        }

                    } else {
                        throw new IllegalArgumentException("Unknown AtomicType: " + atomicType.name());
                    }
                }  // end for

                if (ActionIfUnableToSet.Skip == actionIfUnableToSet) {
                    return Left(CASAtomicRouteTarget.SKIP);

                } else if (ActionIfUnableToSet.Error == actionIfUnableToSet) {
                    // send row to the error output of the step
                    return Left(CASAtomicRouteTarget.ERROR);

                } else if (ActionIfUnableToSet.Loop == actionIfUnableToSet) {

                    // wait before loop to reattempt CaS
                    final long sleptFor = sleepWithTimeout(unableToSetLoopCheckPeriod, waited, unableToSetTimeout);

                    if (sleptFor > 0) {
                        // slept OK
                        waited += sleptFor;
                        // loop to try and match the atomic value again

                    }  else if (sleptFor == 0) {
                        // TIMEOUT reached after sleeping
                        return Left(CASAtomicRouteTarget.TIMEOUT);

                    } else {
                        // Thread INTERRUPTED whilst sleeping
                        return Left(CASAtomicRouteTarget.THREAD_INTERRUPTED);
                    }

                } else {
                    throw new IllegalArgumentException("CAS Unknown ActionIfUnableToSet: " + actionIfUnableToSet.name());
                }

                // refresh the atomic object
                atomicValue = data.getAtomic(atomicId, atomicType);

            }  // end while
        }

        // send to default output if no CAS Target
        return Left(CASAtomicRouteTarget.DEFAULT);
    }

    /**
     * Send row to the 'Skip' output target of the step.
     *
     * @param meta the Step Meta instance
     * @param data the Step Data instance
     * @param atomicId the id of the AtomicValue
     * @param row the row
     *
     * @throws KettleException if the continue target cannot be found, or writing the row causes an error
     */
    protected void putRowToSkipTarget(final CompareAndSetStepMeta meta, final CompareAndSetStepData data, final String atomicId, final Object[] row) throws KettleException {
        // is there a Skip target step?
        final String metaSkipTargetStepName = meta.getSkipTargetStep() != null ? meta.getSkipTargetStep().getName() : meta.getSkipTargetStepname();
        if (isNotEmpty(metaSkipTargetStepName)) {

            // send row to the Skip output of the step
            this.putRowTo(data.getOutputRowMeta(), row, data.getSkipOutputRowSet());
            this.logDebug("CAS SKIP: <{0}>",atomicId);
            logLineNumber();

        } else {
            // raise an exception
            throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.NoSkipTargetStep"));
        }
    }

    @Override
    protected void logLineNumber() {
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

        final StepIOMetaInterface ioMeta = meta.getStepIOMeta();

        // There is one or many CAS target for each target stream.

        final List<StreamInterface> targetStreams = ioMeta.getTargetStreams();
        for (int i = 0; i < targetStreams.size(); i++) {
            final Object subject = targetStreams.get(i).getSubject();
            if (subject == null) {
                continue;  // Skip over default option
            }
            if (!(subject instanceof CompareAndSetTarget)) {
                continue;  // Skip over other target type
            }

            final CompareAndSetTarget compareAndSetValue = (CompareAndSetTarget) subject;

            final String casTargetStepName = compareAndSetValue.getTargetStep() != null ? compareAndSetValue.getTargetStep().getName() : compareAndSetValue.getTargetStepname();
            if (isNullOrEmpty(casTargetStepName)) {
                throw new KettleException(BaseMessages.getString(
                        PKG, "CompareAndSetStep.Log.NoTargetStepSpecifiedForValue", compareAndSetValue.getCompareValue(), compareAndSetValue.getSetValue()));
            }

            final RowSet rowSet = findOutputRowSet(casTargetStepName);
            if (rowSet == null) {
                throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindTargetRowSetForStep", new Object[] { casTargetStepName }));
            }

            // store the compare value and the rowset
            data.getOutputRowSets().put(compareAndSetValue.getCompareValue(), rowSet);
        }


        // The ioMeta object also has optional target streams for: continue, skip, and timeout.

        final String metaContinueTargetStepName = meta.getContinueTargetStep() != null ? meta.getContinueTargetStep().getName() : meta.getContinueTargetStepname();
        if (isNotEmpty(metaContinueTargetStepName)) {
            final RowSet rowSet = findOutputRowSet(metaContinueTargetStepName);
            if (rowSet != null) {
                data.setContinueOutputRowSet(rowSet);
            } else {
                throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindContinueTargetRowSetForStep", new Object[]{ metaContinueTargetStepName }));
            }
        }

        final String metaSkipTargetStepName = meta.getSkipTargetStep() != null ? meta.getSkipTargetStep().getName() : meta.getSkipTargetStepname();
        if (isNotEmpty(metaSkipTargetStepName)) {
            final RowSet rowSet = findOutputRowSet(metaSkipTargetStepName);
            if (rowSet != null) {
                data.setSkipOutputRowSet(rowSet);
            } else {
                throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindSkipTargetRowSetForStep", new Object[]{ metaSkipTargetStepName }));
            }
        }

        final String metaTimeoutTargetStepName = meta.getTimeoutTargetStep() != null ? meta.getTimeoutTargetStep().getName() : meta.getTimeoutTargetStepname();
        if (isNotEmpty(metaTimeoutTargetStepName)) {
            final RowSet rowSet = findOutputRowSet(metaTimeoutTargetStepName);
            if (rowSet != null) {
                data.setTimeoutOutputRowSet(rowSet);
            } else {
                throw new KettleException(BaseMessages.getString(PKG, "CompareAndSetStep.Log.UnableToFindTimeoutTargetRowSetForStep", new Object[] { metaTimeoutTargetStepName }));
            }
        }
    }
}
