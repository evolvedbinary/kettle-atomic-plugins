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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.*;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import uk.gov.nationalarchives.pdi.step.atomics.ActionIfNoAtomic;
import uk.gov.nationalarchives.pdi.step.atomics.AtomicType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static uk.gov.nationalarchives.pdi.step.atomics.Util.*;

@Step(id = "AwaitStep", image = "AwaitStep.svg", name = "Await Atomic Value",
        description = "Waits for an Atomic Value to be Set", categoryDescription = "Flow")
public class AwaitStepMeta extends BaseStepMeta implements StepMetaInterface {

    private static Class<?> PKG = AwaitStep.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    // <editor-fold desc="settings XML element names">
    private static final String ELEM_NAME_ATOMIC_ID_FIELD_NAME = "atomicIdFieldName";
    private static final String ELEM_NAME_ATOMIC_TYPE = "atomicType";
    private static final String ELEM_NAME_ACTION_IF_NO_ATOMIC = "actionIfNoAtomic";
    private static final String ATTR_NAME_CONTINUE_TARGET_STEP = "continueTargetStep";
    private static final String ATTR_NAME_VALUE = "value";
    private static final String ELEM_NAME_ATOMIC_VALUES = "atomicValues";
    private static final String ELEM_NAME_ATOMIC_VALUE = "atomicValue";
    private static final String ATTR_NAME_AWAIT = "await";
    private static final String ATTR_NAME_DISCARD_ATOMIC = "discardAtomic";
    private static final String ATTR_NAME_TARGET_STEP = "targetStep";
    private static final String ELEM_NAME_WAIT_LOOP = "waitLoop";
    private static final String ATTR_NAME_CHECK_PERIOD = "checkPeriod";
    private static final String ATTR_NAME_TIMEOUT = "timeout";
    private static final String ATTR_NAME_TIMEOUT_TARGET_STEP = "timeoutTargetStep";
    // </editor-fold>

    private static final long DEFAULT_CHECK_PERIOD = 100; // ms
    private static final long TIMEOUT_DISABLED = -1; // No timeout
    private static final long DEFAULT_TIMEOUT = TIMEOUT_DISABLED;

    private static final Stream NEW_CONTINUE_STREAM = new Stream(StreamInterface.StreamType.TARGET, (StepMeta)null, BaseMessages.getString(PKG, "AwaitStepMeta.TargetStream.Continue.Description", new String[0]), StreamIcon.TARGET, (Object)null);
    private static final Stream NEW_ATOMIC_VALUE_STREAM = new Stream(StreamInterface.StreamType.TARGET, (StepMeta)null, BaseMessages.getString(PKG, "AwaitStepMeta.TargetStream.AtomicValue.Description", new String[0]), StreamIcon.OUTPUT, (Object)null);
    private static final Stream NEW_TIMEOUT_STREAM = new Stream(StreamInterface.StreamType.TARGET, (StepMeta)null, BaseMessages.getString(PKG, "AwaitStepMeta.TargetStream.Timeout.Description", new String[0]), StreamIcon.FALSE, (Object)null);

    // <editor-fold desc="settings">
    private String atomicIdFieldName;
    private AtomicType atomicType;
    private ActionIfNoAtomic actionIfNoAtomic;
    private String continueTargetStepname;
    @Nullable private String initialiseAtomicValue;
    private long waitAtomicCheckPeriod = DEFAULT_CHECK_PERIOD;
    private long waitAtomicTimeout = DEFAULT_TIMEOUT;
    @Nullable private List<AwaitTarget> awaitValues;
    private long waitLoopCheckPeriod = DEFAULT_CHECK_PERIOD;
    private long waitLoopTimeout = DEFAULT_TIMEOUT;
    private String timeoutTargetStepname;
    // </editor-fold>

    @Nullable private StepMeta continueTargetStep;
    @Nullable private StepMeta timeoutTargetStep;

    @Override
    public void setDefault() {
        atomicIdFieldName = "";
        atomicType = AtomicType.Boolean;
        actionIfNoAtomic = ActionIfNoAtomic.Continue;
        initialiseAtomicValue = null;
        awaitValues = new ArrayList<>();
        waitLoopCheckPeriod = DEFAULT_CHECK_PERIOD;
        waitLoopTimeout = DEFAULT_TIMEOUT;
    }

    @Override
    public String getXML() throws KettleException {
        final StringBuilder builder = new StringBuilder();
        builder
                .append(XMLHandler.addTagValue(ELEM_NAME_ATOMIC_ID_FIELD_NAME, atomicIdFieldName))
                .append(XMLHandler.addTagValue(ELEM_NAME_ATOMIC_TYPE, atomicType.name()));

        if (actionIfNoAtomic == ActionIfNoAtomic.Initialise) {
            builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_NO_ATOMIC, actionIfNoAtomic.name(), true, ATTR_NAME_VALUE, initialiseAtomicValue));
        } else if (actionIfNoAtomic == ActionIfNoAtomic.Wait) {
            builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_NO_ATOMIC, actionIfNoAtomic.name(), true, ATTR_NAME_CHECK_PERIOD, Long.toString(waitAtomicCheckPeriod), ATTR_NAME_TIMEOUT, Long.toString(waitAtomicTimeout)));
        } else if (actionIfNoAtomic == ActionIfNoAtomic.Continue) {
            final String xContinueTargetStepname = this.continueTargetStep != null ? this.continueTargetStep.getName() : this.continueTargetStepname;
            if (!isNullOrEmpty(xContinueTargetStepname)) {
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_NO_ATOMIC, actionIfNoAtomic.name(), true, ATTR_NAME_CONTINUE_TARGET_STEP, xContinueTargetStepname));
            } else {
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_NO_ATOMIC, actionIfNoAtomic.name()));
            }
        } else {
            builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_NO_ATOMIC, actionIfNoAtomic.name()));
        }

        if (awaitValues != null) {
            builder.append(XMLHandler.openTag(ELEM_NAME_ATOMIC_VALUES));
            for (final AwaitTarget awaitValue : awaitValues) {
                final String xTargetStepname = awaitValue.getTargetStep() != null ? awaitValue.getTargetStep().getName() : awaitValue.getTargetStepname();
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ATOMIC_VALUE, null, true, ATTR_NAME_AWAIT, strNullIfNull(nullIfEmpty(awaitValue.getAtomicValue())), ATTR_NAME_DISCARD_ATOMIC, Boolean.toString(awaitValue.isDiscardAtomic()), ATTR_NAME_TARGET_STEP, xTargetStepname));
            }
            builder.append(XMLHandler.closeTag(ELEM_NAME_ATOMIC_VALUES));
        }

        final String xTimeoutTargetStepname = this.timeoutTargetStep != null ? this.timeoutTargetStep.getName() : this.timeoutTargetStepname;
        if (!isNullOrEmpty(xTimeoutTargetStepname)) {
            builder
                    .append(XMLHandler.addTagValue(ELEM_NAME_WAIT_LOOP, null, true, ATTR_NAME_CHECK_PERIOD, Long.toString(waitLoopCheckPeriod), ATTR_NAME_TIMEOUT, Long.toString(waitLoopTimeout), ATTR_NAME_TIMEOUT_TARGET_STEP, xTimeoutTargetStepname));
        } else {
            builder
                    .append(XMLHandler.addTagValue(ELEM_NAME_WAIT_LOOP, null, true, ATTR_NAME_CHECK_PERIOD, Long.toString(waitLoopCheckPeriod), ATTR_NAME_TIMEOUT, Long.toString(waitLoopTimeout)));
        }

        return builder.toString();
    }

    @Override
    public void loadXML(final Node stepnode, final List<DatabaseMeta> databases, final IMetaStore metaStore) throws KettleXMLException {
        final String xAtomicIdFieldName = XMLHandler.getTagValue(stepnode, ELEM_NAME_ATOMIC_ID_FIELD_NAME);
        if (xAtomicIdFieldName != null) {
            this.atomicIdFieldName = xAtomicIdFieldName;

            final String xAtomicType = XMLHandler.getTagValue(stepnode, ELEM_NAME_ATOMIC_TYPE);
            if (xAtomicType != null) {
                try {
                    this.atomicType = AtomicType.valueOf(xAtomicType);
                } catch (final IllegalArgumentException e) {
                    throw new KettleXMLException("Atomic type is invalid: '" + xAtomicType + "': " + e.getMessage(), e);
                }
            }

            final String xActionIfNoAtomic = XMLHandler.getTagValue(stepnode, ELEM_NAME_ACTION_IF_NO_ATOMIC);
            if (xActionIfNoAtomic != null) {
                try {
                    this.actionIfNoAtomic = ActionIfNoAtomic.valueOf(xActionIfNoAtomic);
                } catch (final IllegalArgumentException e) {
                    throw new KettleXMLException("ActionIfNoAtomic is invalid: '" + xActionIfNoAtomic + "': " + e.getMessage(), e);
                }

                if (this.actionIfNoAtomic == ActionIfNoAtomic.Initialise) {
                    final Node node = XMLHandler.getSubNode(stepnode, ELEM_NAME_ACTION_IF_NO_ATOMIC);
                    final String xInitialiseAtomicValue = XMLHandler.getTagAttribute(node, ATTR_NAME_VALUE);
                    if (xInitialiseAtomicValue != null) {
                        this.initialiseAtomicValue = xInitialiseAtomicValue;
                    }
                } else if (this.actionIfNoAtomic == ActionIfNoAtomic.Wait) {
                    final Node node = XMLHandler.getSubNode(stepnode, ELEM_NAME_ACTION_IF_NO_ATOMIC);
                    final String xWaitAtomicCheckPeriod = XMLHandler.getTagAttribute(node, ATTR_NAME_CHECK_PERIOD);
                    if (xWaitAtomicCheckPeriod != null) {
                        try {
                            this.waitAtomicCheckPeriod = Long.valueOf(xWaitAtomicCheckPeriod);
                        } catch (final NumberFormatException e) {
                            throw new KettleXMLException("Wait Atomic Check period '" + waitAtomicCheckPeriod + "' is invalid: " + e.getMessage(), e);
                        }
                    }
                    final String xWaitAtomicTimeout = XMLHandler.getTagAttribute(node, ATTR_NAME_TIMEOUT);
                    if (xWaitAtomicTimeout != null) {
                        try {
                            this.waitAtomicTimeout = Long.valueOf(xWaitAtomicTimeout);
                        } catch (final NumberFormatException e) {
                            throw new KettleXMLException("Wait Atomic Timeout '" + xWaitAtomicTimeout + "' is invalid: " + e.getMessage(), e);
                        }
                    }
                } else if (this.actionIfNoAtomic == ActionIfNoAtomic.Continue) {
                    final Node node = XMLHandler.getSubNode(stepnode, ELEM_NAME_ACTION_IF_NO_ATOMIC);
                    final String xContinueTargetStepname = XMLHandler.getTagAttribute(node, ATTR_NAME_CONTINUE_TARGET_STEP);
                    if (xContinueTargetStepname != null) {
                        this.continueTargetStepname = xContinueTargetStepname;
                    }
                }
            }

            final Node nAtomicValues = XMLHandler.getSubNode(stepnode, ELEM_NAME_ATOMIC_VALUES);
            if (nAtomicValues != null) {
                this.awaitValues = new ArrayList<>();
                final List<Node> nlAtomicValue = XMLHandler.getNodes(nAtomicValues, ELEM_NAME_ATOMIC_VALUE);
                if (nlAtomicValue != null) {
                    for (final Node nAtomicValue : nlAtomicValue) {
                        final NamedNodeMap attrs = nAtomicValue.getAttributes();
                        if (attrs != null) {
                            final Node nAwait = attrs.getNamedItem(ATTR_NAME_AWAIT);
                            final Node nDiscardAtomic = attrs.getNamedItem(ATTR_NAME_DISCARD_ATOMIC);
                            final Node nTarget = attrs.getNamedItem(ATTR_NAME_TARGET_STEP);
                            if (nAwait != null && nDiscardAtomic != null) {
                                final AwaitTarget awaitValue = new AwaitTarget(nullIfStrNull(nullIfEmpty(nAwait.getNodeValue())), Boolean.valueOf(nDiscardAtomic.getNodeValue()), nTarget == null ? null : nTarget.getNodeValue());
                                this.awaitValues.add(awaitValue);
                            }
                        }
                    }
                }
            }

            final Node nWaitLoop = XMLHandler.getSubNode(stepnode, ELEM_NAME_WAIT_LOOP);
            if (nWaitLoop != null) {
                final NamedNodeMap attrs = nWaitLoop.getAttributes();
                final Node nWaitLoopCheckPeriod = attrs.getNamedItem(ATTR_NAME_CHECK_PERIOD);
                final Node nWaitLoopTimeout = attrs.getNamedItem(ATTR_NAME_TIMEOUT);
                if (nWaitLoopCheckPeriod != null && nWaitLoopTimeout != null) {
                    try {
                        this.waitLoopCheckPeriod = Long.parseLong(nWaitLoopCheckPeriod.getNodeValue());
                    } catch (final NumberFormatException e) {
                        throw new KettleXMLException("Wait Loop Check period '" + nWaitLoopCheckPeriod.getNodeValue() + "' is invalid: " + e.getMessage(), e);
                    }
                }
                if (nWaitLoopTimeout != null) {
                    try {
                        this.waitLoopTimeout = Long.parseLong(nWaitLoopTimeout.getNodeValue());
                    } catch (final NumberFormatException e) {
                        throw new KettleXMLException("Wait Loop Timeout '" + nWaitLoopTimeout.getNodeValue() + "' is invalid: " + e.getMessage(), e);
                    }
                }

                final String xTimeoutTargetStepname = XMLHandler.getTagAttribute(stepnode, ATTR_NAME_TIMEOUT_TARGET_STEP);
                if (xTimeoutTargetStepname != null) {
                    this.timeoutTargetStepname = xTimeoutTargetStepname;
                }
            }
        }
    }

    @Override
    public void saveRep(final Repository repo, final IMetaStore metaStore, final ObjectId id_transformation, final ObjectId id_step)
            throws KettleException {

        final String rep = getXML();
        repo.saveStepAttribute(id_transformation, id_step, "step-xml", rep);
    }

    @Override
    public void readRep(final Repository repo, final IMetaStore metaStore, final ObjectId id_step, final List<DatabaseMeta> databases) throws KettleException {
        final String rep = repo.getStepAttributeString(id_step, "step-xml");
        if (rep == null || rep.isEmpty()) {
            setDefault();
        }

        final Node stepnode = XMLHandler.loadXMLString(rep);
        loadXML(stepnode, (List<DatabaseMeta>)null, (IMetaStore)null);
    }

    @Override
    public void check(final List<CheckResultInterface> remarks, final TransMeta transMeta,
                      final StepMeta stepMeta, final RowMetaInterface prev, final String input[], final String output[],
                      final RowMetaInterface info, final VariableSpace space, final Repository repository,
                      final IMetaStore metaStore) {

        final StepIOMetaInterface ioMeta = this.getStepIOMeta();
        final List<StreamInterface> targetStreams = ioMeta.getTargetStreams();
        for (final StreamInterface targetStream : targetStreams) {
            final Object subject = targetStream.getSubject();
            if (subject != null && subject instanceof AwaitTarget) {
                final AwaitTarget awaitValue = (AwaitTarget) subject;
                if (awaitValue.getTargetStep() == null) {
                    final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "AwaitStepMeta.CheckResult.TargetStepInvalid", new String[]{"false", awaitValue.getTargetStepname()}), stepMeta);
                    remarks.add(cr);
                }
            }
        }

        if (prev == null || prev.size() == 0) {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(PKG, "AwaitStepMeta.CheckResult.NotReceivingFields"), stepMeta);
            remarks.add(cr);
        } else {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "AwaitStepMeta.CheckResult.StepRecevingData", prev.size() + ""), stepMeta);
            remarks.add(cr);
        }

        // See if we have input streams leading to this step!
        if (input.length > 0) {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "AwaitStepMeta.CheckResult.StepRecevingData2"), stepMeta);
            remarks.add(cr);
        } else {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "AwaitStepMeta.CheckResult.NoInputReceivedFromOtherSteps"), stepMeta);
            remarks.add(cr);
        }
    }

    @Override
    public StepInterface getStep(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int cnr, final TransMeta tr, final Trans trans) {
        return new AwaitStep(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new AwaitStepData();
    }

    @Override
    public RepositoryDirectory getRepositoryDirectory() {
        return super.getRepositoryDirectory();
    }

    @Override
    public String getDialogClassName() {
        return "uk.gov.nationalarchives.pdi.step.atomics.await.AwaitStepDialog";
    }

    @Override
    public boolean supportsErrorHandling() {
        return true;
    }

    @Override
    public StepIOMetaInterface getStepIOMeta() {
        StepIOMetaInterface ioMeta = super.getStepIOMeta(false);
        if (ioMeta == null) {
            // NOTE: the StepIOMeta parameters: outputProducer is set to false, and outputDynamic to true to disable the "Main output of step" target, as we will control the next target steps explicitly
            ioMeta = new StepIOMeta(true, false, false, false, false, true);

            if (this.getContinueTargetStep() != null) {
                ((StepIOMetaInterface)ioMeta).addStream(new Stream(StreamInterface.StreamType.TARGET, this.getContinueTargetStep(), BaseMessages.getString(PKG, "AwaitStepMeta.TargetStream.Continue.Description", new String[0]), StreamIcon.TARGET, (Object)null));
            }

            if (awaitValues != null) {
                for (final AwaitTarget awaitValue : awaitValues) {
                    final StreamInterface stream = new Stream(StreamInterface.StreamType.TARGET, awaitValue.getTargetStep(), BaseMessages.getString(PKG, "AwaitStepMeta.TargetStream.AtomicValue.Description", new String[]{ strNullIfNull(nullIfEmpty(awaitValue.getAtomicValue())) }), StreamIcon.TARGET, awaitValue);
                    ((StepIOMetaInterface) ioMeta).addStream(stream);
                }
            }

            if (this.getTimeoutTargetStep() != null) {
                ((StepIOMetaInterface)ioMeta).addStream(new Stream(StreamInterface.StreamType.TARGET, this.getTimeoutTargetStep(), BaseMessages.getString(PKG, "AwaitStepMeta.TargetStream.Timeout.Description", new String[0]), StreamIcon.FALSE, (Object)null));
            }

            this.setStepIOMeta((StepIOMetaInterface)ioMeta);
        }

        return (StepIOMetaInterface)ioMeta;
    }

    @Override
    public void searchInfoAndTargetSteps(final List<StepMeta> steps) {
        final List<StreamInterface> targetStreams = this.getStepIOMeta().getTargetStreams();
        for (final StreamInterface targetStream : targetStreams) {
            final Object subject = targetStream.getSubject();
            if (subject != null && subject instanceof AwaitTarget) {
                final AwaitTarget awaitValue = (AwaitTarget) subject;
                final StepMeta stepMeta = StepMeta.findStep(steps, awaitValue.getTargetStepname());
                awaitValue.setTargetStep(stepMeta);
            } else {
                log.logMinimal("Unexpected Target Stream Subject Type: " + subject);
            }
        }

        this.continueTargetStep = StepMeta.findStep(steps, this.continueTargetStepname);
        this.timeoutTargetStep = StepMeta.findStep(steps, this.timeoutTargetStepname);
        this.resetStepIoMeta();
    }

    @Override
    public List<StreamInterface> getOptionalStreams() {
        final List<StreamInterface> list = new ArrayList();
        if (this.getContinueTargetStep() == null) {
            list.add(NEW_CONTINUE_STREAM);
        }
        if (this.getTimeoutTargetStep() == null) {
            list.add(NEW_TIMEOUT_STREAM);
        }

        list.add(NEW_ATOMIC_VALUE_STREAM);
        return list;
    }

    @Override
    public void handleStreamSelection(final StreamInterface stream) {
        if (stream == NEW_CONTINUE_STREAM) {
            this.setContinueTargetStep(stream.getStepMeta());

        } else if (stream == NEW_ATOMIC_VALUE_STREAM) {
            final AwaitTarget awaitValue;
            if (atomicType == AtomicType.Integer) {
                awaitValue = new AwaitTarget("12345", false, stream.getStepMeta());
            } else {
                awaitValue = new AwaitTarget("true", false, stream.getStepMeta());
            }
            if (this.awaitValues == null) {
                this.awaitValues = new ArrayList<>(1);
            }
            this.awaitValues.add(awaitValue);

        } else if (stream == NEW_TIMEOUT_STREAM) {
            this.setTimeoutTargetStep(stream.getStepMeta());
        }

//        if (stream == newCaseTargetStream) {
//            SwitchCaseTarget target = new SwitchCaseTarget();
//            target.caseTargetStep = stream.getStepMeta();
//            target.caseValue = stream.getStepMeta().getName();
//            this.caseTargets.add(target);
//        }

//        List<StreamInterface> targetStreams = this.getStepIOMeta().getTargetStreams();

//        for(int i = 0; i < targetStreams.size(); ++i) {
//            if (stream == targetStreams.get(i)) {
////                SwitchCaseTarget target = (SwitchCaseTarget)stream.getSubject();
////                if (target == null) {
//                    this.setTimeoutTargetStep(stream.getStepMeta());
////                } else {
////                    target.caseTargetStep = stream.getStepMeta();
////                }
//            }
//        }

        this.resetStepIoMeta();
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

    public @Nullable List<AwaitTarget> getAwaitValues() {
        return awaitValues;
    }

    public void setAwaitValues(@Nullable final List<AwaitTarget> awaitValues) {
        this.awaitValues = awaitValues;
    }

    public long getWaitLoopCheckPeriod() {
        return waitLoopCheckPeriod;
    }

    public void setWaitLoopCheckPeriod(final long waitLoopCheckPeriod) {
        this.waitLoopCheckPeriod = waitLoopCheckPeriod;
    }

    public long getWaitLoopTimeout() {
        return waitLoopTimeout;
    }

    public void setWaitLoopTimeout(final long waitLoopTimeout) {
        this.waitLoopTimeout = waitLoopTimeout;
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
