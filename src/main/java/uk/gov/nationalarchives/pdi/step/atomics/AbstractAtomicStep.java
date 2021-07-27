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

import com.evolvedbinary.j8fu.Either;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import static com.evolvedbinary.j8fu.Either.Left;
import static com.evolvedbinary.j8fu.Either.Right;
import static uk.gov.nationalarchives.pdi.step.atomics.Util.isNotEmpty;

public abstract class AbstractAtomicStep extends BaseStep implements StepInterface {

    protected enum GetAtomicRouteTarget {
        CONTINUE,
        ERROR,
        TIMEOUT,
        THREAD_INTERRUPTED
    }

    public AbstractAtomicStep(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int copyNr,
            final TransMeta transMeta, final Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Get the Atomic ID from the input row.
     *
     * @param data the Step Data instance
     * @param row the row
     *
     * @return the id from the row for the AtomicValue
     *
     * @throws KettleStepException if the Atomic ID is not a String
     */
    protected String getAtomicId(final AbstractAtomicStepData data, final Object[] row) throws KettleException {
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
     * This method internally may loop if {@link AbstractAtomicStepMeta#getActionIfNoAtomic()}
     * is set to {@link ActionIfNoAtomic#Wait}.
     *
     * @param meta the Step Meta instance
     * @param data the Step Data instance
     * @param atomicId the id of the AtomicValue to get from AtomicStorage
     *
     * @return Either a route to target if the AtomicValue cannot be retrieved (or initialised),
     *    or the AtomicValue if it was retrieved (or initialised).
     */
    protected Either<GetAtomicRouteTarget, AtomicValue> getAtomic(final AbstractAtomicStepMeta meta, final AbstractAtomicStepData data, final String atomicId) {
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
     * Send row to the 'Continue' output target of the step.
     *
     * @param meta the Step Meta instance
     * @param data the Step Data instance
     * @param atomicId the id of the AtomicValue
     * @param row the row
     * @param messageIfNoContinueTarget the exception message if there is no continue target
     *
     * @throws KettleException if the continue target cannot be found, or writing the row causes an error
     */
    protected void putRowToContinueTarget(final AbstractAtomicStepMeta meta, final AbstractAtomicStepData data, final String atomicId, final Object[] row, final String messageIfNoContinueTarget) throws KettleException {
        this.logDebug("No Atomic object for id: {0}, and ActionIfNoAtomic == Continue", atomicId);

        // is there a Continue target step?
        final String metaContinueTargetStepName = meta.getContinueTargetStep() != null ? meta.getContinueTargetStep().getName() : meta.getContinueTargetStepname();
        if (isNotEmpty(metaContinueTargetStepName)) {

            // send row to the Continue output of the step
            this.putRowTo(data.getOutputRowMeta(), row, data.getContinueOutputRowSet());

            this.logDebug("No Atomic, CONTINUE: <{0}>", atomicId);
            logLineNumber();

        } else {
            // raise an exception
            throw new KettleException(messageIfNoContinueTarget);
        }
    }

    /**
     * Send row to the 'Timeout' output target of the step.
     *
     * @param meta the Step Meta instance
     * @param data the Step Data instance
     * @param row the row
     * @param messageIfNoTimeoutTarget the exception message if there is no timeout target
     *
     * @throws KettleException if the timeout target cannot be found, or writing the row causes an error
     */
    protected void putRowToTimeoutTarget(final AbstractAtomicStepMeta meta, final AbstractAtomicStepData data, final Object[] row, final String messageIfNoTimeoutTarget) throws KettleException {
        // is there a timeout target step?
        final String metaTimeoutTargetStepName = meta.getTimeoutTargetStep() != null ? meta.getTimeoutTargetStep().getName() : meta.getTimeoutTargetStepname();
        if (isNotEmpty(metaTimeoutTargetStepName)) {

            // send row to the timeout output of the step
            this.putRowTo(data.getOutputRowMeta(), row, data.getTimeoutOutputRowSet());

            logLineNumber();

        } else {
            // raise an exception
            throw new KettleException(messageIfNoTimeoutTarget);
        }
    }

    /**
     * Send row to the 'Error' output target of the step.
     *
     * @param data the Step Data instance
     * @param row the row
     * @param errorCode the error code
     * @param errorMessage a description of the error
     *
     * @throws KettleStepException if writing the row causes an error
     */
    protected void putRowToErrorTarget(final AbstractAtomicStepData data, final Object[] row, final ErrorCode errorCode, final String errorMessage) throws KettleStepException {
        // send row to the error output of the step
        this.putError(data.getOutputRowMeta(), row, 1L, errorMessage, data.getAtomicIdFieldName(), errorCode.getCode());
        logLineNumber();
    }

    /**
     * Send row to the default output target of the step.
     *
     * @param data the Step Data instance
     * @param row the row
     *
     * @throws KettleStepException if writing the row causes an error
     */
    protected void putRowToDefaultTarget(final AbstractAtomicStepData data, final Object[] row) throws KettleStepException {
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
    protected long sleepWithTimeout(final long period, long timeAlreadyWaited, final long timeout) {
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

    protected abstract void logLineNumber();
}
