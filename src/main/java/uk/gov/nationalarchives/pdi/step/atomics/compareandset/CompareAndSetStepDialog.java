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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import uk.gov.nationalarchives.pdi.step.atomics.ActionIfNoAtomic;
import uk.gov.nationalarchives.pdi.step.atomics.ActionIfUnableToSet;
import uk.gov.nationalarchives.pdi.step.atomics.AtomicType;
import uk.gov.nationalarchives.pdi.step.atomics.NumberVerifyListener;

import java.util.ArrayList;
import java.util.List;

import static uk.gov.nationalarchives.pdi.step.atomics.Util.*;

public class CompareAndSetStepDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = CompareAndSetStepMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    private static final int MARGIN_SIZE = 15;
    private static final int LABEL_SPACING = 5;
    private static final int ELEMENT_SPACING = 10;

    private static final int LARGE_FIELD = 350;
    private static final int MEDIUM_FIELD = 250;
    private static final int SMALL_FIELD = 75;

    private CompareAndSetStepMeta meta;

    private ScrolledComposite scrolledComposite;
    private Composite contentComposite;
    private Label wStepNameLabel;
    private Text wStepNameField;
    private Label wAtomicIdLabel;
    private Label wAtomicTypeLabel;
    private Combo wAtomicTypeField;
    private TextVar wAtomicIdField;
    private Label wActionIfNoAtomicLabel;
    private Combo wActionIfNoAtomicField;
    private Label wContinueAtomicTargetLabel;
    private CCombo wContinueAtomicTargetField;
    private Combo wInitialiseAtomicBooleanField;
    private TextVar wInitialiseAtomicIntegerField;
    private Label wWaitAtomicCheckPeriodLabel;
    private Text wWaitAtomicCheckPeriodField;
    private Label wWaitAtomicTimeoutLabel;
    private Text wWaitAtomicTimeoutField;
    private Label wActionIfUnableToSetLabel;
    private Combo wActionIfUnableToSetField;
    private Label wUnableToSetLoopCheckPeriodLabel;
    private Text wUnableToSetLoopCheckPeriodField;
    private Label wUnableToSetLoopTimeoutLabel;
    private Text wUnableToSetLoopTimeoutField;
    private Label wUnableToSetLoopTimeoutTargetLabel;
    private CCombo wUnableToSetLoopTimeoutTargetField;
    private Label wUnableToSetSkipTargetLabel;
    private CCombo wUnableToSetSkipTargetField;
    private TableView wCompareAndSetTableView;
    private ColumnInfo ciCompareValue;
    private ColumnInfo ciSetValue;
    private ColumnInfo ciTargetStep;
    private ModifyListener lsFieldsModify;

    public CompareAndSetStepDialog(final Shell parent, final Object in, final TransMeta tr, final String sname) {
        super(parent, (BaseStepMeta) in, tr, sname);
        meta = (CompareAndSetStepMeta) in;
    }

    @Override
    public String open() {
        //Set up window
        final Shell parent = getParent();
        final Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        shell.setMinimumSize(450, 335);
        props.setLook(shell);
        setShellImage(shell, meta);

        lsFieldsModify = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                meta.setChanged();
            }
        };

        //15 pixel margins
        final FormLayout formLayout = new FormLayout();
        formLayout.marginLeft = MARGIN_SIZE;
        formLayout.marginHeight = MARGIN_SIZE;
        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.Shell.Title"));

        //Build a scrolling composite and a composite for holding all content
        scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL);
        contentComposite = new Composite(scrolledComposite, SWT.NONE);
        final FormLayout contentLayout = new FormLayout();
        contentLayout.marginRight = MARGIN_SIZE;
        contentComposite.setLayout(contentLayout);
        final FormData compositeLayoutData = new FormDataBuilder().fullSize()
                .result();
        contentComposite.setLayoutData(compositeLayoutData);
        props.setLook(contentComposite);

        //Step name label and text field
        wStepNameLabel = new Label(contentComposite, SWT.RIGHT);
        wStepNameLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.Stepname.Label"));
        props.setLook(wStepNameLabel);
        final FormData fdStepNameLabel = new FormDataBuilder().left()
                .top()
                .result();
        wStepNameLabel.setLayoutData(fdStepNameLabel);

        wStepNameField = new Text(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepNameField.setText(stepname);
        props.setLook(wStepNameField);
        wStepNameField.addModifyListener(lsFieldsModify);
        final FormData fdStepName = new FormDataBuilder().left()
                .top(wStepNameLabel, LABEL_SPACING)
                .width(MEDIUM_FIELD)
                .result();
        wStepNameField.setLayoutData(fdStepName);

        //Job icon, centered vertically between the top of the label and the bottom of the field.
        final Label wicon = new Label(contentComposite, SWT.CENTER);
        wicon.setImage(getImage());
        final FormData fdIcon = new FormDataBuilder().right()
                .top(0, 4)
                .bottom(new FormAttachment(wStepNameField, 0, SWT.BOTTOM))
                .result();
        wicon.setLayoutData(fdIcon);
        props.setLook(wicon);

        //Spacer between entry info and content
        final Label topSpacer = new Label(contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR);
        final FormData fdSpacer = new FormDataBuilder().fullWidth()
                .top(wStepNameField, MARGIN_SIZE)
                .result();
        topSpacer.setLayoutData(fdSpacer);

        //Groups for first type of content
        final Group settingsGroup = new Group(contentComposite, SWT.SHADOW_ETCHED_IN);
        settingsGroup.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.GroupText"));
        final FormLayout settingsGroupLayout = new FormLayout();
        settingsGroupLayout.marginWidth = MARGIN_SIZE;
        settingsGroupLayout.marginHeight = MARGIN_SIZE;
        settingsGroup.setLayout(settingsGroupLayout);
        final FormData settingsGroupLayoutData = new FormDataBuilder().fullWidth()
                .top(topSpacer, MARGIN_SIZE)
                .result();
        settingsGroup.setLayoutData(settingsGroupLayoutData);
        props.setLook(settingsGroup);

        // atomic id name label/field
        wAtomicIdLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wAtomicIdLabel);
        wAtomicIdLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldAtomicId"));
        final FormData fdAtomicIdLabel = new FormDataBuilder().left()
                .top()
                .result();
        wAtomicIdLabel.setLayoutData(fdAtomicIdLabel);

        wAtomicIdField = new TextVar(transMeta, settingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wAtomicIdField);
        wAtomicIdField.addModifyListener(lsFieldsModify);
        final FormData fdAtomicIdText = new FormDataBuilder().left(wAtomicIdLabel, LABEL_SPACING)
                .top()
                .width(MEDIUM_FIELD)
                .result();
        wAtomicIdField.setLayoutData(fdAtomicIdText);

        // atomic type label/field
        wAtomicTypeLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wAtomicTypeLabel);
        wAtomicTypeLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.ComboAtomicType"));
        final FormData fdAtomicTypeLabel = new FormDataBuilder().left()
                .top(wAtomicIdLabel, ELEMENT_SPACING)
                .result();
        wAtomicTypeLabel.setLayoutData(fdAtomicTypeLabel);

        wAtomicTypeField = new Combo(settingsGroup, SWT.DROP_DOWN | SWT.READ_ONLY | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        for (final AtomicType atomicType : AtomicType.values()) {
            wAtomicTypeField.add(atomicType.name());
        }
        props.setLook(wAtomicTypeField);
        wAtomicTypeField.addModifyListener(lsFieldsModify);
        wAtomicTypeField.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(final SelectionEvent selectionEvent) {
                final String selected = wAtomicTypeField.getText();
                final AtomicType atomicType = AtomicType.valueOf(selected);

                final String selectedActionIfNoAtomic = wActionIfNoAtomicField.getText();
                final ActionIfNoAtomic actionIfNoAtomic = ActionIfNoAtomic.valueOf(selectedActionIfNoAtomic);
                wInitialiseAtomicBooleanField.setVisible(ActionIfNoAtomic.Initialise == actionIfNoAtomic && atomicType == AtomicType.Boolean);
                wInitialiseAtomicIntegerField.setVisible(ActionIfNoAtomic.Initialise == actionIfNoAtomic && atomicType == AtomicType.Integer);

                for (int rowIdx = 0; rowIdx < wCompareAndSetTableView.getItemCount(); rowIdx++) {
                    String compareValue = wCompareAndSetTableView.getItem(rowIdx, 1);
                    String setValue = wCompareAndSetTableView.getItem(rowIdx, 2);

                    if (AtomicType.Boolean == atomicType) {
                        compareValue = unknownStrToBooleanStr(compareValue);
                        setValue = unknownStrToBooleanStr(setValue);

                    } else if (AtomicType.Integer == atomicType) {
                        compareValue = unknownStrToIntegerStr(compareValue);
                        setValue = unknownStrToIntegerStr(setValue);
                    }

                    wCompareAndSetTableView.setText(compareValue, 1, rowIdx);
                    wCompareAndSetTableView.setText(setValue, 2, rowIdx);
                }

                // TODO(AR) can't figure out how to dynamically change the types of the table columns
                /*
                // use CCOMBO for true/false
                ciCompareValue.setFieldTypeColumn(atomicType == AtomicType.Boolean ? ColumnInfo.COLUMN_TYPE_CCOMBO : ColumnInfo.COLUMN_TYPE_TEXT);
                ciCompareValue.setNumeric(atomicType == AtomicType.Integer);
                ciSetValue.setFieldTypeColumn(atomicType == AtomicType.Boolean ? ColumnInfo.COLUMN_TYPE_CCOMBO : ColumnInfo.COLUMN_TYPE_TEXT);
                ciSetValue.setNumeric(atomicType == AtomicType.Integer);
                wCompareAndSetTableView.setColumnInfo(0, ciCompareValue);
                wCompareAndSetTableView.setColumnInfo(1, ciSetValue);
                wCompareAndSetTableView.redraw();
                wCompareAndSetTableView.update();
                */
            }
        });
        final FormData fdAtomicTypeField = new FormDataBuilder().left(wAtomicTypeLabel, LABEL_SPACING)
                .top(wAtomicIdLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wAtomicTypeField.setLayoutData(fdAtomicTypeField);

        // if no such atomic label/field
        wActionIfNoAtomicLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wActionIfNoAtomicLabel);
        wActionIfNoAtomicLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.IfNoSuchAtomic"));
        final FormData fdActionIfNoAtomicLabel = new FormDataBuilder().left()
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .result();
        wActionIfNoAtomicLabel.setLayoutData(fdActionIfNoAtomicLabel);

        wActionIfNoAtomicField = new Combo(settingsGroup, SWT.DROP_DOWN | SWT.READ_ONLY | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        for (final ActionIfNoAtomic actionIfNoAtomic : ActionIfNoAtomic.values()) {
            wActionIfNoAtomicField.add(actionIfNoAtomic.name());
        }
        props.setLook(wActionIfNoAtomicField);
        wActionIfNoAtomicField.addModifyListener(lsFieldsModify);
        wActionIfNoAtomicField.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(final SelectionEvent selectionEvent) {
                final String selected = wActionIfNoAtomicField.getText();
                final ActionIfNoAtomic actionIfNoAtomic = ActionIfNoAtomic.valueOf(selected);

                final String selectedAtomicType = wAtomicTypeField.getText();
                final AtomicType atomicType = AtomicType.valueOf(selectedAtomicType);

                wContinueAtomicTargetLabel.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Continue);
                wContinueAtomicTargetField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Continue);

                wInitialiseAtomicBooleanField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Initialise && atomicType == AtomicType.Boolean);
                wInitialiseAtomicIntegerField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Initialise && atomicType == AtomicType.Integer);

                wWaitAtomicCheckPeriodLabel.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);
                wWaitAtomicCheckPeriodField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);
                wWaitAtomicTimeoutLabel.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);
                wWaitAtomicTimeoutField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);
            }
        });
        final FormData fdActionIfNoAtomicField = new FormDataBuilder().left(wActionIfNoAtomicLabel, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wActionIfNoAtomicField.setLayoutData(fdActionIfNoAtomicField);

        // continue target label/field
        wContinueAtomicTargetLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wContinueAtomicTargetLabel);
        wContinueAtomicTargetLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldContinueTarget"));
        final FormData fdContinueAtomicTargetLabel = new FormDataBuilder().left(wActionIfNoAtomicField, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .result();
        wContinueAtomicTargetLabel.setLayoutData(fdContinueAtomicTargetLabel);

        wContinueAtomicTargetField = new CCombo(settingsGroup, SWT.DROP_DOWN | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        final String[] nextStepNames = this.transMeta.getNextStepNames(this.stepMeta);
        wContinueAtomicTargetField.setItems(nextStepNames);
        props.setLook(wContinueAtomicTargetField);
        wContinueAtomicTargetField.addModifyListener(lsFieldsModify);
        final FormData fdContinueAtomicTargetField = new FormDataBuilder().left(wContinueAtomicTargetLabel, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .width(MEDIUM_FIELD)
                .result();
        wContinueAtomicTargetField.setLayoutData(fdContinueAtomicTargetField);

        // initialise atomic with boolean field
        wInitialiseAtomicBooleanField = new Combo(settingsGroup, SWT.DROP_DOWN | SWT.READ_ONLY | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wInitialiseAtomicBooleanField.add("false");
        wInitialiseAtomicBooleanField.add("true");
        props.setLook(wInitialiseAtomicBooleanField);
        wInitialiseAtomicBooleanField.addModifyListener(lsFieldsModify);
        final FormData fdInitialiseAtomicBooleanField = new FormDataBuilder().left(wActionIfNoAtomicField, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wInitialiseAtomicBooleanField.setLayoutData(fdInitialiseAtomicBooleanField);

        // initialise atomic with integer field
        wInitialiseAtomicIntegerField = new TextVar(transMeta, settingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wInitialiseAtomicIntegerField);
        wInitialiseAtomicIntegerField.addModifyListener(lsFieldsModify);
        wInitialiseAtomicIntegerField.getTextWidget().addVerifyListener(new NumberVerifyListener(Integer::parseInt));
        final FormData fdInitialiseAtomicField = new FormDataBuilder().left(wActionIfNoAtomicField, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wInitialiseAtomicIntegerField.setLayoutData(fdInitialiseAtomicField);

        // check period label/field
        wWaitAtomicCheckPeriodLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wWaitAtomicCheckPeriodLabel);
        wWaitAtomicCheckPeriodLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldCheckPeriod"));
        final FormData fdWaitAtomicCheckPeriodLabel = new FormDataBuilder().left(wActionIfNoAtomicField, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .result();
        wWaitAtomicCheckPeriodLabel.setLayoutData(fdWaitAtomicCheckPeriodLabel);

        wWaitAtomicCheckPeriodField = new Text(settingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wWaitAtomicCheckPeriodField);
        wWaitAtomicCheckPeriodField.addModifyListener(lsFieldsModify);
        wWaitAtomicCheckPeriodField.addVerifyListener(new NumberVerifyListener(Long::parseLong));
        final FormData fdWaitAtomicCheckPeriodField = new FormDataBuilder().left(wWaitAtomicCheckPeriodLabel, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wWaitAtomicCheckPeriodField.setLayoutData(fdWaitAtomicCheckPeriodField);

        // timeout label/field
        wWaitAtomicTimeoutLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wWaitAtomicTimeoutLabel);
        wWaitAtomicTimeoutLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldTimeout"));
        final FormData fdWaitAtomicTimeoutLabel = new FormDataBuilder().left(wWaitAtomicCheckPeriodField, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .result();
        wWaitAtomicTimeoutLabel.setLayoutData(fdWaitAtomicTimeoutLabel);

        wWaitAtomicTimeoutField = new Text(settingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wWaitAtomicTimeoutField);
        wWaitAtomicTimeoutField.addModifyListener(lsFieldsModify);
        wWaitAtomicTimeoutField.addVerifyListener(new NumberVerifyListener(Long::parseLong));
        final FormData fdWaitAtomicTimeoutField = new FormDataBuilder().left(wWaitAtomicTimeoutLabel, LABEL_SPACING)
                .top(wAtomicTypeLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wWaitAtomicTimeoutField.setLayoutData(fdWaitAtomicTimeoutField);

        // action if unable to set label/field
        wActionIfUnableToSetLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wActionIfUnableToSetLabel);
        wActionIfUnableToSetLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.ComboActionIfUnableToSet"));
        final FormData fdActionIfUnableToSetLabel = new FormDataBuilder().left()
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .result();
        wActionIfUnableToSetLabel.setLayoutData(fdActionIfUnableToSetLabel);

        wActionIfUnableToSetField = new Combo(settingsGroup, SWT.DROP_DOWN | SWT.READ_ONLY | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        for (final ActionIfUnableToSet actionIfUnableToSet : ActionIfUnableToSet.values()) {
            wActionIfUnableToSetField.add(actionIfUnableToSet.name());
        }
        props.setLook(wActionIfUnableToSetField);
        wActionIfUnableToSetField.addModifyListener(lsFieldsModify);
        wActionIfUnableToSetField.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(final SelectionEvent selectionEvent) {
                final String selected = wActionIfUnableToSetField.getText();
                final ActionIfUnableToSet actionIfUnableToSet = ActionIfUnableToSet.valueOf(selected);

                wUnableToSetLoopCheckPeriodLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
                wUnableToSetLoopCheckPeriodField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
                wUnableToSetLoopTimeoutLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
                wUnableToSetLoopTimeoutField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
                wUnableToSetLoopTimeoutTargetLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
                wUnableToSetLoopTimeoutTargetField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);

                wUnableToSetSkipTargetLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Skip);
                wUnableToSetSkipTargetField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Skip);
            }
        });
        final FormData fdActionIfUnableToSetField = new FormDataBuilder().left(wActionIfUnableToSetLabel, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wActionIfUnableToSetField.setLayoutData(fdActionIfUnableToSetField);

        // check period label/field
        wUnableToSetLoopCheckPeriodLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wUnableToSetLoopCheckPeriodLabel);
        wUnableToSetLoopCheckPeriodLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldCheckPeriod"));
        final FormData fdCheckPeriodLabel = new FormDataBuilder().left(wActionIfUnableToSetField, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .result();
        wUnableToSetLoopCheckPeriodLabel.setLayoutData(fdCheckPeriodLabel);

        wUnableToSetLoopCheckPeriodField = new Text(settingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wUnableToSetLoopCheckPeriodField);
        wUnableToSetLoopCheckPeriodField.addModifyListener(lsFieldsModify);
        wUnableToSetLoopCheckPeriodField.addVerifyListener(new NumberVerifyListener(Long::parseLong));
        final FormData fdUnableToSetLoopCheckPeriodField = new FormDataBuilder().left(wUnableToSetLoopCheckPeriodLabel, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wUnableToSetLoopCheckPeriodField.setLayoutData(fdUnableToSetLoopCheckPeriodField);

        // timeout label/field
        wUnableToSetLoopTimeoutLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wUnableToSetLoopTimeoutLabel);
        wUnableToSetLoopTimeoutLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldTimeout"));
        final FormData fdUnableToSetLoopTimeoutLabel = new FormDataBuilder().left(wUnableToSetLoopCheckPeriodField, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .result();
        wUnableToSetLoopTimeoutLabel.setLayoutData(fdUnableToSetLoopTimeoutLabel);

        wUnableToSetLoopTimeoutField = new Text(settingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wUnableToSetLoopTimeoutField);
        wUnableToSetLoopTimeoutField.addModifyListener(lsFieldsModify);
        wUnableToSetLoopTimeoutField.addVerifyListener(new NumberVerifyListener(Long::parseLong));
        final FormData fdTimeoutField = new FormDataBuilder().left(wUnableToSetLoopTimeoutLabel, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wUnableToSetLoopTimeoutField.setLayoutData(fdTimeoutField);

        // timeout target label/field
        wUnableToSetLoopTimeoutTargetLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wUnableToSetLoopTimeoutTargetLabel);
        wUnableToSetLoopTimeoutTargetLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldTimeoutTarget"));
        final FormData fdUnableToSetLoopTimeoutTargetLabel = new FormDataBuilder().left(wUnableToSetLoopTimeoutField, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .result();
        wUnableToSetLoopTimeoutTargetLabel.setLayoutData(fdUnableToSetLoopTimeoutTargetLabel);

        wUnableToSetLoopTimeoutTargetField = new CCombo(settingsGroup, SWT.DROP_DOWN | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wUnableToSetLoopTimeoutTargetField.setItems(nextStepNames);
        props.setLook(wUnableToSetLoopTimeoutTargetField);
        wUnableToSetLoopTimeoutTargetField.addModifyListener(lsFieldsModify);
        final FormData fdUnableToSetLoopTimeoutTargetField = new FormDataBuilder().left(wUnableToSetLoopTimeoutTargetLabel, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .width(MEDIUM_FIELD)
                .result();
        wUnableToSetLoopTimeoutTargetField.setLayoutData(fdUnableToSetLoopTimeoutTargetField);

        // skip target label/field
        wUnableToSetSkipTargetLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wUnableToSetSkipTargetLabel);
        wUnableToSetSkipTargetLabel.setText(BaseMessages.getString(PKG, "CompareAndSetStepDialog.TextFieldSkipTarget"));
        final FormData fdUnableToSetSkipTargetLabel = new FormDataBuilder().left(wActionIfUnableToSetField, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .result();
        wUnableToSetSkipTargetLabel.setLayoutData(fdUnableToSetSkipTargetLabel);

        wUnableToSetSkipTargetField = new CCombo(settingsGroup, SWT.DROP_DOWN | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wUnableToSetSkipTargetField.setItems(nextStepNames);
        props.setLook(wUnableToSetSkipTargetField);
        wUnableToSetSkipTargetField.addModifyListener(lsFieldsModify);
        final FormData fdUnableToSetSkipTargetField = new FormDataBuilder().left(wUnableToSetSkipTargetLabel, LABEL_SPACING)
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .width(MEDIUM_FIELD)
                .result();
        wUnableToSetSkipTargetField.setLayoutData(fdUnableToSetSkipTargetField);

        // compare and set table
        ciCompareValue = new ColumnInfo(
                BaseMessages.getString(PKG, "CompareAndSetStepDialog.CompareValue"),
                ColumnInfo.COLUMN_TYPE_TEXT,
                false
        );

        ciSetValue = new ColumnInfo(
                BaseMessages.getString(PKG, "CompareAndSetStepDialog.SetValue"),
                ColumnInfo.COLUMN_TYPE_TEXT,
                false
        );

        ciTargetStep = new ColumnInfo(
                BaseMessages.getString(PKG, "CompareAndSetStepDialog.TargetStep"),
                ColumnInfo.COLUMN_TYPE_CCOMBO,
                nextStepNames
        );

        final ColumnInfo[] compareAndSetTableColumns = {
                ciCompareValue,
                ciSetValue,
                ciTargetStep
        };

        wCompareAndSetTableView = new TableView(
                transMeta, settingsGroup, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
                compareAndSetTableColumns,1, lsFieldsModify, props);
        final FormData fdCompareAndSetTableView = new FormDataBuilder().left()
                .top(wActionIfUnableToSetLabel, ELEMENT_SPACING)
                .fullWidth()
                .height(ELEMENT_SPACING * 10)
                .result();
        wCompareAndSetTableView.setLayoutData(fdCompareAndSetTableView);


        //Cancel and OK buttons for the bottom of the window.
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
        final FormData fdCancel = new FormDataBuilder().right(100, -MARGIN_SIZE)
                .bottom()
                .result();
        wCancel.setLayoutData(fdCancel);

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        final FormData fdOk = new FormDataBuilder().right(wCancel, -LABEL_SPACING)
                .bottom()
                .result();
        wOK.setLayoutData(fdOk);

        //Space between bottom buttons and the table, final layout for table
        final Label bottomSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
        final FormData fdhSpacer = new FormDataBuilder().left()
                .right(100, -MARGIN_SIZE)
                .bottom(wCancel, -MARGIN_SIZE)
                .result();
        bottomSpacer.setLayoutData(fdhSpacer);

        //Add everything to the scrolling composite
        scrolledComposite.setContent(contentComposite);
        scrolledComposite.setExpandVertical(true);
        scrolledComposite.setExpandHorizontal(true);
        scrolledComposite.setMinSize(contentComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));

        scrolledComposite.setLayout(new FormLayout());
        final FormData fdScrolledComposite = new FormDataBuilder().fullWidth()
                .top()
                .bottom(bottomSpacer, -MARGIN_SIZE * 4)
                .result();
        scrolledComposite.setLayoutData(fdScrolledComposite);
        props.setLook(scrolledComposite);

        //Listeners
        lsCancel = new Listener() {
            @Override
            public void handleEvent(final Event e) {
                cancel();
            }
        };
        lsOK = new Listener() {
            @Override
            public void handleEvent(final Event e) {
                ok();
            }
        };

        wOK.addListener(SWT.Selection, lsOK);
        wCancel.addListener(SWT.Selection, lsCancel);

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(final SelectionEvent e) {
                ok();
            }
        };
        wStepNameField.addSelectionListener(lsDef);

        shell.addShellListener(new ShellAdapter() {
            public void shellClosed(final ShellEvent e) {
                cancel();
            }
        });

        //Show shell
        setSize();
        getData(meta);
        meta.setChanged(changed);
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        changed = meta.hasChanged();

        return stepname;
    }

    private Image getImage() {
        final PluginInterface plugin =
                PluginRegistry.getInstance().getPlugin(StepPluginType.class, stepMeta.getStepMetaInterface());
        final String id = plugin.getIds()[0];
        if (id != null) {
            return GUIResource.getInstance().getImagesSteps().get(id).getAsBitmapForSize(shell.getDisplay(),
                    ConstUI.ICON_SIZE, ConstUI.ICON_SIZE);
        }
        return null;
    }

    private void cancel() {
        dispose();
    }

    private void ok() {
        // SAVE DATA
        saveData();

        // NOTIFY CHANGE
        meta.setChanged(true);

        stepname = wStepNameField.getText();
        dispose();
    }

    private void getData(final CompareAndSetStepMeta meta) {
        final String atomicId = meta.getAtomicIdFieldName();
        if (atomicId != null) {
            wAtomicIdField.setText(atomicId);
        }

        AtomicType atomicType = meta.getAtomicType();
        if (atomicType == null) {
            atomicType = AtomicType.Boolean;
        }
        wAtomicTypeField.setText(atomicType.name());

        ActionIfNoAtomic actionIfNoAtomic = meta.getActionIfNoAtomic();
        if (actionIfNoAtomic == null) {
            actionIfNoAtomic = ActionIfNoAtomic.Continue;
        }
        wActionIfNoAtomicField.setText(actionIfNoAtomic.name());
        wContinueAtomicTargetField.setText(meta.getContinueTargetStep() == null ? "" : meta.getContinueTargetStep().getName());
        wContinueAtomicTargetLabel.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Continue);
        wContinueAtomicTargetField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Continue);
        final String initialiseAtomicValue = meta.getInitialiseAtomicValue();
        if (initialiseAtomicValue != null) {
            if (AtomicType.Boolean == atomicType) {
                wInitialiseAtomicBooleanField.setText(initialiseAtomicValue);
                wInitialiseAtomicIntegerField.setText("0");
            } else if (AtomicType.Integer == atomicType) {
                wInitialiseAtomicIntegerField.setText(initialiseAtomicValue);
                wInitialiseAtomicBooleanField.setText("false");
            }
        } else {
            wInitialiseAtomicBooleanField.setText("false");
            wInitialiseAtomicIntegerField.setText("0");
        }
        wInitialiseAtomicBooleanField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Initialise && atomicType == AtomicType.Boolean);
        wInitialiseAtomicIntegerField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Initialise && atomicType == AtomicType.Integer);
        wWaitAtomicCheckPeriodField.setText(Long.toString(meta.getWaitAtomicCheckPeriod()));
        wWaitAtomicCheckPeriodLabel.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);
        wWaitAtomicCheckPeriodField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);
        wWaitAtomicTimeoutField.setText(Long.toString(meta.getWaitAtomicTimeout()));
        wWaitAtomicTimeoutLabel.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);
        wWaitAtomicTimeoutField.setVisible(actionIfNoAtomic == ActionIfNoAtomic.Wait);

        // use CCOMBO for true/false
        ciCompareValue.setFieldTypeColumn(atomicType == AtomicType.Boolean ? ColumnInfo.COLUMN_TYPE_CCOMBO : ColumnInfo.COLUMN_TYPE_TEXT);
        ciCompareValue.setNumeric(atomicType == AtomicType.Integer);
        ciSetValue.setFieldTypeColumn(atomicType == AtomicType.Boolean ? ColumnInfo.COLUMN_TYPE_CCOMBO : ColumnInfo.COLUMN_TYPE_TEXT);
        ciSetValue.setNumeric(atomicType == AtomicType.Integer);

        ActionIfUnableToSet actionIfUnableToSet = meta.getActionIfUnableToSet();
        if (actionIfUnableToSet == null) {
            actionIfUnableToSet = ActionIfUnableToSet.Error;
        }
        wActionIfUnableToSetField.setText(actionIfUnableToSet.name());
        wUnableToSetLoopCheckPeriodField.setText(Long.toString(meta.getUnableToSetLoopCheckPeriod()));
        wUnableToSetLoopTimeoutField.setText(Long.toString(meta.getUnableToSetLoopTimeout()));
        wUnableToSetLoopTimeoutTargetField.setText(meta.getTimeoutTargetStep() == null ? "" : meta.getTimeoutTargetStep().getName());
        wUnableToSetSkipTargetField.setText(meta.getSkipTargetStep() == null ? "" : meta.getSkipTargetStep().getName());

        wUnableToSetLoopCheckPeriodLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
        wUnableToSetLoopCheckPeriodField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
        wUnableToSetLoopTimeoutLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
        wUnableToSetLoopTimeoutField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
        wUnableToSetLoopTimeoutTargetLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
        wUnableToSetLoopTimeoutTargetField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Loop);
        wUnableToSetSkipTargetLabel.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Skip);
        wUnableToSetSkipTargetField.setVisible(actionIfUnableToSet == ActionIfUnableToSet.Skip);


        final List<CompareAndSetTarget> compareAndSetValues = meta.getCompareAndSetValues();
        if (compareAndSetValues != null) {
            wCompareAndSetTableView.getTable().removeAll();
            for (final CompareAndSetTarget compareAndSetValue : compareAndSetValues) {
                // TODO(AR) fix atomic types?
                final String targetStepname = compareAndSetValue.getTargetStep() == null ? "" : compareAndSetValue.getTargetStep().getName();
                wCompareAndSetTableView.add(new String[]  {compareAndSetValue.getCompareValue(), compareAndSetValue.getSetValue(),  targetStepname});
            }
        }
    }

    private void saveData() {
        final ActionIfNoAtomic actionIfNoAtomic;
        try {
            actionIfNoAtomic = ActionIfNoAtomic.valueOf(wActionIfNoAtomicField.getText());
        } catch (final IllegalArgumentException e) {
            //TODO(AR) show an error to the user
            throw e;
        }

        final AtomicType atomicType;
        try {
            atomicType = AtomicType.valueOf(wAtomicTypeField.getText());
        } catch (final IllegalArgumentException e) {
            //TODO(AR) show an error to the user
            throw e;
        }

        final ActionIfUnableToSet actionIfUnableToSet;
        try {
            actionIfUnableToSet = ActionIfUnableToSet.valueOf(wActionIfUnableToSetField.getText());
        } catch (final IllegalArgumentException e) {
            //TODO(AR) show an error to the user
            throw e;
        }

        final int compareAndSetValuesLen = wCompareAndSetTableView.getItemCount();
        final List<CompareAndSetTarget> compareAndSetValues = new ArrayList<>(compareAndSetValuesLen);
        for (int i = 0; i < compareAndSetValuesLen; i++) {
            final String compare = wCompareAndSetTableView.getItem(i, 1);
            final String set = wCompareAndSetTableView.getItem(i, 2);
            if (compare != null && set != null) {
                try {
                    final StepMeta targetStep;
                    final String targetStepName = wCompareAndSetTableView.getItem(i, 3);
                    if (!isNullOrEmpty(targetStepName)) {
                        targetStep = transMeta.findStep(targetStepName);
                    } else {
                        targetStep = null;
                    }

                    compareAndSetValues.add(new CompareAndSetTarget(atomicType.checkValidValue(compare), atomicType.checkValidValue(set), targetStep));
                } catch (final IllegalArgumentException e) {
                    //TODO(AR) show an error to the user
                    throw e;
                }
            }
        }

        meta.setAtomicIdFieldName(wAtomicIdField.getText());
        meta.setAtomicType(atomicType);
        meta.setActionIfNoAtomic(actionIfNoAtomic);
        if (ActionIfNoAtomic.Continue == actionIfNoAtomic) {
            final String continueTargetName = this.wContinueAtomicTargetField.getText();
            if (!isNullOrEmpty(continueTargetName)) {
                final StepMeta continueTargetStep = transMeta.findStep(continueTargetName);
                meta.setContinueTargetStep(continueTargetStep);
            } else {
                meta.setContinueTargetStep(null);
            }
        } else if (ActionIfNoAtomic.Initialise == actionIfNoAtomic) {
            if (AtomicType.Boolean == atomicType) {
                meta.setInitialiseAtomicValue(wInitialiseAtomicBooleanField.getText());
            } else if (AtomicType.Integer == atomicType) {
                meta.setInitialiseAtomicValue(wInitialiseAtomicIntegerField.getText());
            }
        } else if (ActionIfNoAtomic.Wait == actionIfNoAtomic) {
            try {
                final long waitAtomicCheckPeriod = Long.parseLong(wWaitAtomicCheckPeriodField.getText());
                final long waitAtomicTimeout = Long.parseLong(wWaitAtomicTimeoutField.getText());
                meta.setWaitAtomicCheckPeriod(waitAtomicCheckPeriod);
                meta.setWaitAtomicTimeout(waitAtomicTimeout);
            } catch (final NumberFormatException e) {
                //TODO(AR) show an error to the user
                throw e;
            }
        }
        meta.setActionIfUnableToSet(actionIfUnableToSet);
        if (ActionIfUnableToSet.Loop == actionIfUnableToSet) {
            try {
                final long unableToSetLoopCheckPeriod = Long.parseLong(wUnableToSetLoopCheckPeriodField.getText());
                final long unableToSetLoopTimeout = Long.parseLong(wUnableToSetLoopTimeoutField.getText());
                meta.setUnableToSetLoopCheckPeriod(unableToSetLoopCheckPeriod);
                meta.setUnableToSetLoopTimeout(unableToSetLoopTimeout);

                final String timeoutTargetName = this.wUnableToSetLoopTimeoutTargetField.getText();
                if (!isNullOrEmpty(timeoutTargetName)) {
                    final StepMeta timeoutTargetStep = transMeta.findStep(timeoutTargetName);
                    meta.setTimeoutTargetStep(timeoutTargetStep);
                } else {
                    meta.setTimeoutTargetStep(null);
                }
            } catch (final NumberFormatException e) {
                //TODO(AR) show an error to the user
                throw e;
            }
        } else if (ActionIfUnableToSet.Skip == actionIfUnableToSet) {
            final String skipTargetName = this.wUnableToSetSkipTargetField.getText();
            if (!isNullOrEmpty(skipTargetName)) {
                final StepMeta skipTargetStep = transMeta.findStep(skipTargetName);
                meta.setSkipTargetStep(skipTargetStep);
            } else {
                meta.setSkipTargetStep(null);
            }
        }

        meta.setCompareAndSetValues(compareAndSetValues);
    }
}
