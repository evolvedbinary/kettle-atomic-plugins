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

import com.evolvedbinary.j8fu.Either;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
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

import static com.evolvedbinary.j8fu.Either.Left;
import static com.evolvedbinary.j8fu.Either.Right;
import static uk.gov.nationalarchives.pdi.step.atomics.Util.*;

public class AwaitStep extends BaseStep implements StepInterface {

    private enum GetAtomicRouteTarget {
        CONTINUE,
        ERROR,
        TIMEOUT,
        THREAD_INTERRUPTED
    }

    private enum AwaitAtomicRouteTarget {
        DEFAULT,
        TIMEOUT,
        THREAD_INTERRUPTED
    }

    static final String IGNORE_STEPNAME_FOR_TEST = "__IGNORE_STEPNAME_FOR_TEST__";

    private static final Class<?> PKG = AwaitStep.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    public AwaitStep(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int copyNr,
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
        final AwaitStepMeta meta = (AwaitStepMeta) smi;
        final AwaitStepData data = (AwaitStepData) sdi;

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
                    putRowToContinueTarget(meta, data, atomicId, row);
                    return true;

                case ERROR:
                    putRowToErrorTarget(data, row, ErrorCode.NO_SUCH_ATOMIC, "No Atomic object for id: " + atomicId + ", and ActionIfNoAtomic == Error");
                    return true;

                case TIMEOUT:
                    // NOTE: this is intentionally sent to the error target at this stage, the timeout target is reserved for the await value part further below
                    putRowToErrorTarget(data, row, ErrorCode.NO_SUCH_ATOMIC_WAIT_TIMEOUT, "Timeout (" + meta.getWaitAtomicTimeout() + "ms) exceeded whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait");
                    return true;

                case THREAD_INTERRUPTED:
                    putRowToErrorTarget(data, row, ErrorCode.NO_SUCH_ATOMIC_WAIT_INTERRUPTED, "Thread interrupted whilst waiting for Atomic object creation for id: " + atomicId + ", and ActionIfNoAtomic == Wait");
                    return true;
            }
        }

        // At this point we have an AtomicValue
        final AtomicValue atomicValue = routeOrAtomic.right().get();

        // 2. Check/Wait until the AtomicValue reaches one of the await values, and then get the target
        final Either<AwaitAtomicRouteTarget, AwaitTarget> routeOrAwaitTarget = awaitAndGetTarget(meta, data, atomicId, atomicValue);
        if (routeOrAwaitTarget.isLeft()) {
            // AtomicValue never reached one of the await values, so route row to specific failure output target...
            final AwaitAtomicRouteTarget route = routeOrAwaitTarget.left().get();
            switch (route) {
                case DEFAULT:
                    putRowToDefaultTarget(data, row);
                    return true;

                case TIMEOUT:
                    putRowToTimeoutTarget(meta, data, row);
                    return true;

                case THREAD_INTERRUPTED:
                    putRowToErrorTarget(data, row, ErrorCode.AWAIT_ATOMIC_WAIT_INTERRUPTED, "Thread interrupted whilst waiting for Atomic value for id: " + atomicId);
                    return true;
            }
        }

        // At this point we have an AwaitTarget, i.e. the AtomicValue matches an await value
        final AwaitTarget awaitTarget = routeOrAwaitTarget.right().get();

        // We now send the input row to specific targets for Await success
        final Set<RowSet> atomicValueTargetRowSets = data.getAtomicValueOutputRowSets().get(awaitTarget.getAtomicValue());
        if (atomicValueTargetRowSets == null || atomicValueTargetRowSets.isEmpty()) {
            throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.UnableToFindTargetRowSetForStep", new Object[] { awaitTarget.getTargetStep() != null ? awaitTarget.getTargetStep().getName() : awaitTarget.getTargetStepname() }));
        }

        // send the row to the success targets
        for (final RowSet awaitValueTargetRowSet : atomicValueTargetRowSets) {
            this.putRowTo(data.getOutputRowMeta(), row, awaitValueTargetRowSet);
        }

        this.logDebug("Await DONE: <{0}>[{1}]", atomicId, strNullIfNull(nullIfEmpty(awaitTarget.getAtomicValue())));

        if (awaitTarget.isDiscardAtomic()) {
            // discard the atomic if user requested to do so
            if (!data.removeAtomic(atomicId)) {
                this.logError("Unable to discard Atomic with ID: {0}", atomicId);
            }
        }

        logLineNumber();

        return true;  // row done!
    }

    /**
     * Get the Atomic ID from the input row.
     *
     * @param data the Await Step Data instance
     * @param row the row
     *
     * @return the id from the row for the AtomicValue
     *
     * @throws KettleStepException if the Atomic ID is not a String
     */
    private String getAtomicId(final AwaitStepData data, final Object[] row) throws KettleException {
        final Object atomicIdFieldNameValue = row[data.getAtomicIdFieldIndex()];
        if (atomicIdFieldNameValue instanceof String) {
            return (String) atomicIdFieldNameValue;
        } else {
            throw new KettleException("Expected field " + data.getAtomicIdFieldName() + " to contain a String, but found "
                    + atomicIdFieldNameValue.getClass());
        }
    }

    /**
     * Attempts to get (or initialise) the AtomicValue from {@link AtomicStorage}.
     *
     * This method internally may loop if {@link AwaitStepMeta#getActionIfNoAtomic()}
     * is set to {@link ActionIfNoAtomic#Wait}.
     *
     * @param meta the Await Step Meta instance
     * @param data the Await Step Data instance
     * @param atomicId the id of the AtomicValue to get from AtomicStorage
     *
     * @return Either a route to target if the AtomicValue cannot be retrieved (or initialised),
     *    or the AtomicValue if it was retrieved (or initialised).
     */
    private Either<GetAtomicRouteTarget, AtomicValue> getAtomic(final AwaitStepMeta meta, final AwaitStepData data, final String atomicId) {
        final ActionIfNoAtomic actionIfNoAtomic = meta.getActionIfNoAtomic();
        final AtomicType atomicType = meta.getAtomicType();
        final long waitAtomicCheckPeriod = meta.getWaitAtomicCheckPeriod();
        final long waitAtomicTimeout = meta.getWaitAtomicTimeout();

        long waitedForAtomic = 0;

        while (true) {
            final AtomicValue atomicValue;
            if (ActionIfNoAtomic.Initialise == actionIfNoAtomic) {
                atomicValue = data.getOrCreateAtomic(atomicId, atomicType, meta.getInitialiseAtomicValue());
            } else {
                atomicValue = data.getAtomic(atomicId, atomicType);
            }

            if (atomicValue != null) {
                return Right(atomicValue);  // got atomicValue.. exit while loop and return it
            }


            /*
                atomicValue is null, we must now check how to proceed...
            */

            if (ActionIfNoAtomic.Continue == actionIfNoAtomic) {
                return Left(GetAtomicRouteTarget.CONTINUE);

            } else if (ActionIfNoAtomic.Error == actionIfNoAtomic) {
                return Left(GetAtomicRouteTarget.ERROR);

            } else if (ActionIfNoAtomic.Wait == actionIfNoAtomic) {
                final long sleptFor = sleepWithTimeout(waitAtomicCheckPeriod, waitedForAtomic, waitAtomicTimeout);
                if (sleptFor > 0) {
                    // slept OK
                    waitedForAtomic += sleptFor;
                    // loop to try and get the atomic again

                } else if (sleptFor == 0) {
                    // TIMEOUT reached after sleeping
                    return Left(GetAtomicRouteTarget.TIMEOUT);

                } else {
                    // Thread INTERRUPTED whilst sleeping
                    return Left(GetAtomicRouteTarget.THREAD_INTERRUPTED);
                }
            }
        }  // end while
    }

    /**
     * Attempts to wait for the AtomicValue to become one of the await values.
     *
     * This method internally will loop approximately every {@link AwaitStepMeta#getWaitLoopCheckPeriod()}
     * until the AtomicValue matches one of the await values, or {@link AwaitStepMeta#getWaitLoopTimeout()} is reached.
     *
     * @param meta the Await Step Meta instance
     * @param data the Await Step Data instance
     * @param atomicId the id of the AtomicValue
     * @param atomicValue the AtomicValue on which we await to reach a specific value
     *
     * @return Either a route to target if the AtomicValue never matches one of the await values,
     *    or the AwaitTarget to route the output to when it matches one of the await values.
     */
    private Either<AwaitAtomicRouteTarget, AwaitTarget> awaitAndGetTarget(final AwaitStepMeta meta, final AwaitStepData data, final String atomicId, AtomicValue atomicValue) {
        final AtomicType atomicType = meta.getAtomicType();
        final List<AwaitTarget> awaitValues = meta.getAwaitValues();
        if (awaitValues != null && !awaitValues.isEmpty()) {

            // check if one of the options is to await for `null`
            final AwaitTarget awaitNullTarget = AwaitTarget.findAwaitTargetForNullValue(awaitValues);

            final long waitLoopCheckPeriod = meta.getWaitLoopCheckPeriod();
            final long waitLoopTimeout = meta.getWaitLoopTimeout();

            long waited = 0;
            while (true) {

                if (awaitNullTarget != null && atomicValue == null) {
                    // null is a valid value to check for, i.e. already discarded
                    return Right(awaitNullTarget);

                } else {

                    for (final AwaitTarget awaitValue : awaitValues) {
                        @Nullable final String awaitAtomicValue = awaitValue.getAtomicValue();

                        if (awaitAtomicValue == null) {
                            // handled above in awaitNullTarget logic
                            continue;

                        } else if (AtomicType.Boolean == atomicType) {
                            final AtomicBooleanValue atomicBoolean = (AtomicBooleanValue) atomicValue;
                            final boolean awaitValueBoolean = Boolean.parseBoolean(awaitAtomicValue);
                            if (awaitValueBoolean == atomicBoolean.get()) {
                                return Right(awaitValue);
                            }

                        } else if (AtomicType.Integer == atomicType) {
                            final int awaitValueInt = Integer.parseInt(awaitAtomicValue);
                            final AtomicIntegerValue atomicInteger = (AtomicIntegerValue) atomicValue;
                            if (awaitValueInt == atomicInteger.get()) {
                                return Right(awaitValue);
                            }

                        } else {
                            throw new IllegalArgumentException("Unknown AtomicType: " + atomicType.name());
                        }

                    }  // end for
                }

                // wait and check again
                final long sleptFor = sleepWithTimeout(waitLoopCheckPeriod, waited, waitLoopTimeout);

                if (sleptFor > 0) {
                    // slept OK
                    waited += sleptFor;
                    // loop to try and match the atomic value again

                }  else if (sleptFor == 0) {
                    // TIMEOUT reached after sleeping
                    return Left(AwaitAtomicRouteTarget.TIMEOUT);

                } else {
                    // Thread INTERRUPTED whilst sleeping
                    return Left(AwaitAtomicRouteTarget.THREAD_INTERRUPTED);
                }

                // refresh the atomic object
                atomicValue = data.getAtomic(atomicId, atomicType);

            }  // end while
        }

        // send to default output if no Await Target
        return Left(AwaitAtomicRouteTarget.DEFAULT);
    }

    /**
     * Send row to the 'Continue' output target of the step.
     *
     * @param meta the Await Step Meta instance
     * @param data the Await Step Data instance
     * @param atomicId the id of the AtomicValue
     * @param row the row
     *
     * @throws KettleException if the continue target cannot be found, or writing the row causes an error
     */
    private void putRowToContinueTarget(final AwaitStepMeta meta, final AwaitStepData data, final String atomicId, final Object[] row) throws KettleException {
        this.logDebug("Await No Atomic object for id: {0}, and ActionIfNoAtomic == Continue", atomicId);

        // is there a Continue target step?
        final String metaContinueTargetStepName = meta.getContinueTargetStep() != null ? meta.getContinueTargetStep().getName() : meta.getContinueTargetStepname();
        if (isNotEmpty(metaContinueTargetStepName)) {

            // send row to the Continue output of the step
            this.putRowTo(data.getOutputRowMeta(), row, data.getContinueOutputRowSet());

            this.logDebug("Await No Atomic, CONTINUE: <{0}>", atomicId);
            logLineNumber();

        } else {
            // raise an exception
            throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.NoContinueTargetStep"));
        }
    }

    /**
     * Send row to the 'Timeout' output target of the step.
     *
     * @param meta the Await Step Meta instance
     * @param data the Await Step Data instance
     * @param row the row
     *
     * @throws KettleException if the timeout target cannot be found, or writing the row causes an error
     */
    private void putRowToTimeoutTarget(final AwaitStepMeta meta, final AwaitStepData data, final Object[] row) throws KettleException {
        // is there a timeout target step?
        final String metaTimeoutTargetStepName = meta.getTimeoutTargetStep() != null ? meta.getTimeoutTargetStep().getName() : meta.getTimeoutTargetStepname();
        if (isNotEmpty(metaTimeoutTargetStepName)) {

            // send row to the timeout output of the step
            this.putRowTo(data.getOutputRowMeta(), row, data.getTimeoutOutputRowSet());

            logLineNumber();

        } else {
            // raise an exception
            throw new KettleException(BaseMessages.getString(PKG, "AwaitStep.Log.NoTimeoutTargetStep"));
        }
    }

    /**
     * Send row to the 'Error' output target of the step.
     *
     * @param data the Await Step Data instance
     * @param row the row
     * @param errorCode the error code
     * @param errorMessage a description of the error
     *
     * @throws KettleStepException if writing the row causes an error
     */
    private void putRowToErrorTarget(final AwaitStepData data, final Object[] row, final ErrorCode errorCode, final String errorMessage) throws KettleStepException {
        // send row to the error output of the step
        this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), errorCode.getCode());
        logLineNumber();
    }

    /**
     * Send row to the default output target of the step.
     *
     * @param data the Await Step Data instance
     * @param row the row
     *
     * @throws KettleStepException if writing the row causes an error
     */
    private void putRowToDefaultTarget(final AwaitStepData data, final Object[] row) throws KettleStepException {
        this.putRow(data.getOutputRowMeta(), row);
        logLineNumber();
    }

    /**
     * Sleeps and then tests if a timeout has been exceeded.
     *
     * @param period the period to sleep for
     * @param timeAlreadyWaited the amount of time previously waited for, e.g. if this function is called more than
     *                          once in a loop then the result of this function should be added to
     *                          {@code timeAlreadyWaited} and fed back into this parameter on the next call to it.
     *                          Can be set to 0 to indicate no previous wait.
     * @param timeout the timeout to check for, the test is {@code timeout != -1 && (timeAlreadyWaited + period) > timeout}.
     *                Can be set to -1 to disable any timeout check, in which case 0 will never be returned from this function.
     *
     * @return -1 indicates that the (sleeping) Thread was interrupted,
     *          0 indicates that the timeout was exceeded,
     *          a non-zero value is the {@code period} waited
     */
    private long sleepWithTimeout(final long period, long timeAlreadyWaited, final long timeout) {
        try {
            Thread.sleep(period);
            timeAlreadyWaited += period;
            if (timeout != -1 && timeAlreadyWaited > timeout) {
                return 0;
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupted flag
            return -1;
        }
        return period;
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

        final StepIOMetaInterface ioMeta = meta.getStepIOMeta();

        // There is one or many CAS target for each target stream.
        final List<StreamInterface> targetStreams = ioMeta.getTargetStreams();
        for (final StreamInterface targetStream : targetStreams) {
            final Object subject = targetStream.getSubject();
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
    }
}
