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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
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
import uk.gov.nationalarchives.pdi.step.atomics.ActionIfUnableToSet;
import uk.gov.nationalarchives.pdi.step.atomics.AtomicType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static uk.gov.nationalarchives.pdi.step.atomics.Util.isNullOrEmpty;

@Step(id = "CompareAndSetStep", image = "CompareAndSetStep.svg", name = "Compare And Set Atomic Value",
        description = "Compare and Set an Atomic Value", categoryDescription = "Flow")
public class CompareAndSetStepMeta extends BaseStepMeta implements StepMetaInterface {

    private static Class<?> PKG = CompareAndSetStep.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    // <editor-fold desc="settings XML element names">
    private static final String ELEM_NAME_ATOMIC_ID_FIELD_NAME = "atomicIdFieldName";
    private static final String ELEM_NAME_ATOMIC_TYPE = "atomicType";
    private static final String ELEM_NAME_ACTION_IF_NO_ATOMIC = "actionIfNoAtomic";
    private static final String ATTR_NAME_CONTINUE_TARGET_STEP = "continueTargetStep";
    private static final String ATTR_NAME_CHECK_PERIOD = "checkPeriod";
    private static final String ATTR_NAME_TIMEOUT = "timeout";
    private static final String ATTR_NAME_VALUE = "value";
    private static final String ELEM_NAME_ACTION_IF_UNABLE_TO_SET = "actionIfUnableToSet";
    private static final String ATTR_NAME_SKIP_TARGET_STEP = "skipTargetStep";
    private static final String ATTR_NAME_TIMEOUT_TARGET_STEP = "timeoutTargetStep";
    private static final String ELEM_NAME_ATOMIC_VALUES = "atomicValues";
    private static final String ELEM_NAME_ATOMIC_VALUE = "atomicValue";
    private static final String ATTR_NAME_COMPARE = "compare";
    private static final String ATTR_NAME_SET = "set";
    private static final String ATTR_NAME_TARGET_STEP = "targetStep";
    // </editor-fold>

    private static final long DEFAULT_CHECK_PERIOD = 100; // ms
    private static final long TIMEOUT_DISABLED = -1; // No timeout
    private static final long DEFAULT_TIMEOUT = TIMEOUT_DISABLED;

    private static final Stream NEW_CONTINUE_STREAM = new Stream(StreamInterface.StreamType.TARGET, (StepMeta)null, BaseMessages.getString(PKG, "CompareAndSetStepMeta.TargetStream.Continue.Description", new String[0]), StreamIcon.TARGET, (Object)null);
    private static final Stream NEW_SKIP_STREAM = new Stream(StreamInterface.StreamType.TARGET, (StepMeta)null, BaseMessages.getString(PKG, "CompareAndSetStepMeta.TargetStream.Skip.Description", new String[0]), StreamIcon.TARGET, (Object)null);
    private static final Stream NEW_TIMEOUT_STREAM = new Stream(StreamInterface.StreamType.TARGET, (StepMeta)null, BaseMessages.getString(PKG, "CompareAndSetStepMeta.TargetStream.Timeout.Description", new String[0]), StreamIcon.FALSE, (Object)null);
    private static final Stream NEW_CAS_TARGET_STREAM = new Stream(StreamInterface.StreamType.TARGET, (StepMeta)null, BaseMessages.getString(PKG, "CompareAndSetStepMeta.TargetStream.NewCASTarget.Description", new String[0]), StreamIcon.TARGET, (Object)null);

    // <editor-fold desc="settings">
    private String atomicIdFieldName;
    private AtomicType atomicType;
    private ActionIfNoAtomic actionIfNoAtomic;
    private String continueTargetStepname;
    @Nullable private String initialiseAtomicValue;
    private long waitAtomicCheckPeriod = DEFAULT_CHECK_PERIOD;
    private long waitAtomicTimeout = DEFAULT_TIMEOUT;
    private ActionIfUnableToSet actionIfUnableToSet;
    private String skipTargetStepname;
    private long unableToSetLoopCheckPeriod = DEFAULT_CHECK_PERIOD;
    private long unableToSetLoopTimeout = DEFAULT_TIMEOUT;
    private String timeoutTargetStepname;
    @Nullable private List<CompareAndSetTarget> compareAndSetValues;
    // </editor-fold>

    @Nullable private StepMeta continueTargetStep;
    @Nullable private StepMeta skipTargetStep;
    @Nullable private StepMeta timeoutTargetStep;

    @Override
    public void setDefault() {
        atomicIdFieldName = "";
        atomicType = AtomicType.Boolean;
        actionIfNoAtomic = ActionIfNoAtomic.Continue;
        initialiseAtomicValue = null;
        waitAtomicCheckPeriod = DEFAULT_CHECK_PERIOD;
        waitAtomicTimeout = DEFAULT_TIMEOUT;
        actionIfUnableToSet = ActionIfUnableToSet.Error;
        unableToSetLoopCheckPeriod = DEFAULT_CHECK_PERIOD;
        unableToSetLoopTimeout = DEFAULT_TIMEOUT;
        compareAndSetValues = new ArrayList<>();
    }

    @Override
    public Object clone() {
        final CompareAndSetStepMeta retval = (CompareAndSetStepMeta) super.clone();
        retval.compareAndSetValues = new ArrayList<>();
        try {
            if (this.compareAndSetValues != null) {
                for (final CompareAndSetTarget compareAndSetValue : this.compareAndSetValues) {
                    retval.compareAndSetValues.add((CompareAndSetTarget) compareAndSetValue.clone());
                }
            }
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
        return retval;
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

        if (actionIfUnableToSet == ActionIfUnableToSet.Loop) {
            final String xTimeoutTargetStepname = this.timeoutTargetStep != null ? this.timeoutTargetStep.getName() : this.timeoutTargetStepname;
            if (!isNullOrEmpty(xTimeoutTargetStepname)) {
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_UNABLE_TO_SET, actionIfUnableToSet.name(), true, ATTR_NAME_CHECK_PERIOD, Long.toString(unableToSetLoopCheckPeriod), ATTR_NAME_TIMEOUT, Long.toString(unableToSetLoopTimeout), ATTR_NAME_TIMEOUT_TARGET_STEP, xTimeoutTargetStepname));
            } else {
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_UNABLE_TO_SET, actionIfUnableToSet.name(), true, ATTR_NAME_CHECK_PERIOD, Long.toString(unableToSetLoopCheckPeriod), ATTR_NAME_TIMEOUT, Long.toString(unableToSetLoopTimeout)));
            }
        } else if (actionIfUnableToSet == ActionIfUnableToSet.Skip) {
            final String xSkipTargetStepname = this.skipTargetStep != null ? this.skipTargetStep.getName() : this.skipTargetStepname;
            if (!isNullOrEmpty(xSkipTargetStepname)) {
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_UNABLE_TO_SET, actionIfUnableToSet.name(), true, ATTR_NAME_SKIP_TARGET_STEP, xSkipTargetStepname));
            } else {
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_UNABLE_TO_SET, actionIfUnableToSet.name()));
            }
        } else {
            builder.append(XMLHandler.addTagValue(ELEM_NAME_ACTION_IF_UNABLE_TO_SET, actionIfUnableToSet.name()));
        }

        if (compareAndSetValues != null) {
            builder.append(XMLHandler.openTag(ELEM_NAME_ATOMIC_VALUES));
            for (final CompareAndSetTarget compareAndSetValue : compareAndSetValues) {
                final String xTargetStepname = compareAndSetValue.getTargetStep() != null ? compareAndSetValue.getTargetStep().getName() : compareAndSetValue.getTargetStepname();
                builder.append(XMLHandler.addTagValue(ELEM_NAME_ATOMIC_VALUE, null, true, ATTR_NAME_COMPARE, compareAndSetValue.getCompareValue(), ATTR_NAME_SET, compareAndSetValue.getSetValue(), ATTR_NAME_TARGET_STEP, xTargetStepname));
            }
            builder.append(XMLHandler.closeTag(ELEM_NAME_ATOMIC_VALUES));
        }

        return builder.toString();
    }

    @Override
    public void loadXML(final Node stepnode, final List<DatabaseMeta> databases, final IMetaStore metaStore) throws KettleXMLException {
        final String xAtomicId = XMLHandler.getTagValue(stepnode, ELEM_NAME_ATOMIC_ID_FIELD_NAME);
        if (xAtomicId != null) {
            this.atomicIdFieldName = xAtomicId;

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

            final String xActionIfUnableToSet = XMLHandler.getTagValue(stepnode, ELEM_NAME_ACTION_IF_UNABLE_TO_SET);
            if (xActionIfUnableToSet != null) {
                try {
                    this.actionIfUnableToSet = ActionIfUnableToSet.valueOf(xActionIfUnableToSet);
                } catch (final IllegalArgumentException e) {
                    throw new KettleXMLException("ActionIfUnableToSet is invalid: '" + xActionIfUnableToSet + "': " + e.getMessage(), e);
                }

                if (this.actionIfUnableToSet == ActionIfUnableToSet.Loop) {
                    final Node node = XMLHandler.getSubNode(stepnode, ELEM_NAME_ACTION_IF_UNABLE_TO_SET);
                    final String xUnableToSetLoopCheckPeriod = XMLHandler.getTagAttribute(node, ATTR_NAME_CHECK_PERIOD);
                    if (xUnableToSetLoopCheckPeriod != null) {
                        try {
                            this.unableToSetLoopCheckPeriod = Long.valueOf(xUnableToSetLoopCheckPeriod);
                        } catch (final NumberFormatException e) {
                            throw new KettleXMLException("Check period '" + xUnableToSetLoopCheckPeriod + "' is invalid: " + e.getMessage(), e);
                        }
                    }
                    final String xUnableToSetLoopTimeout = XMLHandler.getTagAttribute(node, ATTR_NAME_TIMEOUT);
                    if (xUnableToSetLoopTimeout != null) {
                        try {
                            this.unableToSetLoopTimeout = Long.valueOf(xUnableToSetLoopTimeout);
                        } catch (final NumberFormatException e) {
                            throw new KettleXMLException("Timeout '" + xUnableToSetLoopTimeout + "' is invalid: " + e.getMessage(), e);
                        }
                    }
                    final String xTimeoutTargetStepname = XMLHandler.getTagAttribute(node, ATTR_NAME_TIMEOUT_TARGET_STEP);
                    if (xTimeoutTargetStepname != null) {
                        this.timeoutTargetStepname = xTimeoutTargetStepname;
                    }
                } else if (this.actionIfUnableToSet == ActionIfUnableToSet.Skip) {
                    final Node node = XMLHandler.getSubNode(stepnode, ELEM_NAME_ACTION_IF_UNABLE_TO_SET);
                    final String xSkipTargetStepname = XMLHandler.getTagAttribute(node, ATTR_NAME_SKIP_TARGET_STEP);
                    if (xSkipTargetStepname != null) {
                        this.skipTargetStepname = xSkipTargetStepname;
                    }
                }
            }

            final Node nAtomicValues = XMLHandler.getSubNode(stepnode, ELEM_NAME_ATOMIC_VALUES);
            if (nAtomicValues != null) {
                this.compareAndSetValues = new ArrayList<>();
                final List<Node> nlAtomicValue = XMLHandler.getNodes(nAtomicValues, ELEM_NAME_ATOMIC_VALUE);
                if (nlAtomicValue != null) {
                    for (final Node nAtomicValue : nlAtomicValue) {
                        final NamedNodeMap attrs = nAtomicValue.getAttributes();
                        if (attrs != null) {
                            final Node nCompare = attrs.getNamedItem(ATTR_NAME_COMPARE);
                            final Node nSet = attrs.getNamedItem(ATTR_NAME_SET);
                            final Node nTarget = attrs.getNamedItem(ATTR_NAME_TARGET_STEP);
                            if (nCompare != null && nSet != null) {
                                final CompareAndSetTarget compareAndSetValue = new CompareAndSetTarget(nCompare.getNodeValue(), nSet.getNodeValue(), nTarget == null ? null : nTarget.getNodeValue());
                                this.compareAndSetValues.add(compareAndSetValue);
                            }
                        }
                    }
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
            final CompareAndSetTarget compareAndSetValue = (CompareAndSetTarget)targetStream.getSubject();
            if (compareAndSetValue != null && compareAndSetValue.getTargetStep() == null) {
                final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "CompareAndSetStepMeta.CheckResult.TargetStepInvalid", new String[]{"false", compareAndSetValue.getTargetStepname()}), stepMeta);
                remarks.add(cr);
            }
        }

        if (prev == null || prev.size() == 0) {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(PKG, "CompareAndSetStepMeta.CheckResult.NotReceivingFields"), stepMeta);
            remarks.add(cr);
        } else {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "CompareAndSetStepMeta.CheckResult.StepRecevingData", prev.size() + ""), stepMeta);
            remarks.add(cr);
        }

        // See if we have input streams leading to this step!
        if (input.length > 0) {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "CompareAndSetStepMeta.CheckResult.StepRecevingData2"), stepMeta);
            remarks.add(cr);
        } else {
            final CheckResult cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "CompareAndSetStepMeta.CheckResult.NoInputReceivedFromOtherSteps"), stepMeta);
            remarks.add(cr);
        }
    }

    @Override
    public StepInterface getStep(final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int cnr, final TransMeta tr, final Trans trans) {
        return new CompareAndSetStep(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new CompareAndSetStepData();
    }

    @Override
    public RepositoryDirectory getRepositoryDirectory() {
        return super.getRepositoryDirectory();
    }

    @Override
    public String getDialogClassName() {
        return "uk.gov.nationalarchives.pdi.step.atomics.compareandset.CompareAndSetStepDialog";
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

            if (compareAndSetValues != null) {
                for (final CompareAndSetTarget compareAndSetValue : compareAndSetValues) {
                    final StreamInterface stream = new Stream(StreamInterface.StreamType.TARGET, compareAndSetValue.getTargetStep(), BaseMessages.getString(PKG, "CompareAndSetStepMeta.TargetStream.CASTarget.Description", new String[]{Const.NVL(compareAndSetValue.getCompareValue(), ""), Const.NVL(compareAndSetValue.getSetValue(), "")}), StreamIcon.TARGET, compareAndSetValue);
                    ((StepIOMetaInterface) ioMeta).addStream(stream);
                }
            }

            if (this.getSkipTargetStep() != null) {
                ((StepIOMetaInterface)ioMeta).addStream(new Stream(StreamInterface.StreamType.TARGET, this.getSkipTargetStep(), BaseMessages.getString(PKG, "CompareAndSetStepMeta.TargetStream.Skip.Description", new String[0]), StreamIcon.TARGET, (Object)null));
            }

            if (this.getTimeoutTargetStep() != null) {
                ((StepIOMetaInterface)ioMeta).addStream(new Stream(StreamInterface.StreamType.TARGET, this.getTimeoutTargetStep(), BaseMessages.getString(PKG, "CompareAndSetStepMeta.TargetStream.Timeout.Description", new String[0]), StreamIcon.FALSE, (Object)null));
            }

            this.setStepIOMeta((StepIOMetaInterface)ioMeta);
        }

        return (StepIOMetaInterface)ioMeta;
    }

    @Override
    public void searchInfoAndTargetSteps(final List<StepMeta> steps) {
        final List<StreamInterface> targetStreams = this.getStepIOMeta().getTargetStreams();
        for (final StreamInterface targetStream : targetStreams) {
            final CompareAndSetTarget compareAndSetValue = (CompareAndSetTarget)targetStream.getSubject();
            if (compareAndSetValue != null) {
                final StepMeta stepMeta = StepMeta.findStep(steps, compareAndSetValue.getTargetStepname());
                compareAndSetValue.setTargetStep(stepMeta);
            }
        }

        this.continueTargetStep = StepMeta.findStep(steps, this.continueTargetStepname);
        this.skipTargetStep = StepMeta.findStep(steps, this.skipTargetStepname);
        this.timeoutTargetStep = StepMeta.findStep(steps, this.timeoutTargetStepname);
        this.resetStepIoMeta();
    }

    @Override
    public List<StreamInterface> getOptionalStreams() {
        final List<StreamInterface> list = new ArrayList();
        if (this.getContinueTargetStep() == null) {
            list.add(NEW_CONTINUE_STREAM);
        }
        if (this.getSkipTargetStep() == null) {
            list.add(NEW_SKIP_STREAM);
        }
        if (this.getTimeoutTargetStep() == null) {
            list.add(NEW_TIMEOUT_STREAM);
        }

        list.add(NEW_CAS_TARGET_STREAM);
        return list;
    }

    @Override
    public void handleStreamSelection(final StreamInterface stream) {
        if (stream == NEW_CONTINUE_STREAM) {
            this.setContinueTargetStep(stream.getStepMeta());

        } else if (stream == NEW_SKIP_STREAM) {
            this.setSkipTargetStep(stream.getStepMeta());

        } else if (stream == NEW_TIMEOUT_STREAM) {
            this.setTimeoutTargetStep(stream.getStepMeta());

        } else if (stream == NEW_CAS_TARGET_STREAM) {
            final CompareAndSetTarget compareAndSetValue = new CompareAndSetTarget("compare1", "set1", stream.getStepMeta());
            if (this.compareAndSetValues == null) {
                this.compareAndSetValues = new ArrayList<>(1);
            }
            this.compareAndSetValues.add(compareAndSetValue);
        }

        final  List<StreamInterface> targetStreams = this.getStepIOMeta().getTargetStreams();

        for(int i = 0; i < targetStreams.size(); ++i) {
            if (stream == targetStreams.get(i)) {
                final CompareAndSetTarget compareAndSetValue = (CompareAndSetTarget)stream.getSubject();
                if (compareAndSetValue != null) {
                    compareAndSetValue.setTargetStep(stream.getStepMeta());
                }
            }
        }

        this.resetStepIoMeta();
    }

    @Override
    public boolean excludeFromCopyDistributeVerification() {
        return true;
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

    public ActionIfUnableToSet getActionIfUnableToSet() {
        return actionIfUnableToSet;
    }

    public void setActionIfUnableToSet(final ActionIfUnableToSet actionIfUnableToSet) {
        this.actionIfUnableToSet = actionIfUnableToSet;
    }

    public String getSkipTargetStepname() {
        return skipTargetStepname;
    }

    public void setSkipTargetStepname(final String skipTargetStepname) {
        this.skipTargetStepname = skipTargetStepname;
    }

    @Nullable public StepMeta getSkipTargetStep() {
        return skipTargetStep;
    }

    public void setSkipTargetStep(@Nullable final StepMeta skipTargetStep) {
        this.skipTargetStep = skipTargetStep;
    }

    public long getUnableToSetLoopCheckPeriod() {
        return unableToSetLoopCheckPeriod;
    }

    public void setUnableToSetLoopCheckPeriod(final long unableToSetLoopCheckPeriod) {
        this.unableToSetLoopCheckPeriod = unableToSetLoopCheckPeriod;
    }

    public long getUnableToSetLoopTimeout() {
        return unableToSetLoopTimeout;
    }

    public void setUnableToSetLoopTimeout(final long unableToSetLoopTimeout) {
        this.unableToSetLoopTimeout = unableToSetLoopTimeout;
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

    public @Nullable List<CompareAndSetTarget> getCompareAndSetValues() {
        return compareAndSetValues;
    }

    public void setCompareAndSetValues(@Nullable final List<CompareAndSetTarget> compareAndSetValues) {
        this.compareAndSetValues = compareAndSetValues;
    }

    // </editor-fold>
}
