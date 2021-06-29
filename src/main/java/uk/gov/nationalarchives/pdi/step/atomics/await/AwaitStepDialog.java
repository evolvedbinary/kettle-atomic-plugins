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
import uk.gov.nationalarchives.pdi.step.atomics.AtomicType;
import uk.gov.nationalarchives.pdi.step.atomics.NumberVerifyListener;

import java.util.ArrayList;
import java.util.List;

import static uk.gov.nationalarchives.pdi.step.atomics.Util.*;

public class AwaitStepDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = AwaitStepMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    private static final int MARGIN_SIZE = 15;
    private static final int LABEL_SPACING = 5;
    private static final int ELEMENT_SPACING = 10;

    private static final int LARGE_FIELD = 350;
    private static final int MEDIUM_FIELD = 250;
    private static final int SMALL_FIELD = 75;

    private AwaitStepMeta meta;

    private ScrolledComposite scrolledComposite;
    private Composite contentComposite;
    private Label wStepNameLabel;
    private Text wStepNameField;
    private Label wAtomicIdLabel;
    private TextVar wAtomicIdField;
    private Label wAtomicTypeLabel;
    private Combo wAtomicTypeField;
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
    private TableView wAwaitTableView;
    private ColumnInfo ciAtomicValue;
    private ColumnInfo ciDiscardAtomic;
    private ColumnInfo ciTargetStep;

    private Label wWaitLoopCheckPeriodLabel;
    private Text wWaitLoopCheckPeriodField;
    private Label wWaitLoopTimeoutLabel;
    private Text wWaitLoopTimeoutField;
    private Label wTimeoutTargetLabel;
    private CCombo wTimeoutTargetField;
    private ModifyListener lsFieldsModify;

    public AwaitStepDialog(final Shell parent, final Object in, final TransMeta tr, final String sname) {
        super(parent, (BaseStepMeta) in, tr, sname);
        meta = (AwaitStepMeta) in;
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
        shell.setText(BaseMessages.getString(PKG, "AwaitStepDialog.Shell.Title"));

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
        wStepNameLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.Stepname.Label"));
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

        // Group for settings
        final Group settingsGroup = new Group(contentComposite, SWT.SHADOW_ETCHED_IN);
        props.setLook(settingsGroup);
        settingsGroup.setText(BaseMessages.getString(PKG, "AwaitStepDialog.GroupText.Settings"));
        final FormLayout settingsGroupLayout = new FormLayout();
        settingsGroupLayout.marginWidth = MARGIN_SIZE;
        settingsGroupLayout.marginHeight = MARGIN_SIZE;
        settingsGroup.setLayout(settingsGroupLayout);
        final FormData settingsGroupLayoutData = new FormDataBuilder().fullWidth()
                .top(topSpacer, MARGIN_SIZE)
                .result();
        settingsGroup.setLayoutData(settingsGroupLayoutData);

        // atomic id name label/field
        wAtomicIdLabel = new Label(settingsGroup, SWT.LEFT);
        props.setLook(wAtomicIdLabel);
        wAtomicIdLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.TextFieldAtomicId"));
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
        wAtomicTypeLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.ComboAtomicType"));
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

                for (int rowIdx = 0; rowIdx < wAwaitTableView.getItemCount(); rowIdx++) {
                    String atomicValue = wAwaitTableView.getItem(rowIdx, 1);
                    atomicValue = strNullIfNull(atomicValue);

                    if (!"null".equals(atomicValue)) {
                        if (AtomicType.Boolean == atomicType) {
                            atomicValue = unknownStrToBooleanStr(atomicValue);

                        } else if (AtomicType.Integer == atomicType) {
                            atomicValue = unknownStrToIntegerStr(atomicValue);
                        }
                    }

                    wAwaitTableView.setText(atomicValue, 1, rowIdx);
                }

                // TODO(AR) can't figure out how to dynamically change the types of the table columns
                /*
                // use CCOMBO for true/false
                ciAtomicValue.setFieldTypeColumn(atomicType == AtomicType.Boolean ? ColumnInfo.COLUMN_TYPE_CCOMBO : ColumnInfo.COLUMN_TYPE_TEXT);
                wAwaitTableView.setColumnInfo(0, ciAtomicValue);
                wAwaitTableView.redraw();
                wAwaitTableView.update();
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
        wActionIfNoAtomicLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.IfNoSuchAtomic"));
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
        wContinueAtomicTargetLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.TextFieldContinueTarget"));
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
        wWaitAtomicCheckPeriodLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.TextFieldCheckPeriod"));
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
        wWaitAtomicTimeoutLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.TextFieldTimeout"));
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

        // await table
        ciAtomicValue = new ColumnInfo(
                BaseMessages.getString(PKG, "AwaitStepDialog.AtomicValue"),
                ColumnInfo.COLUMN_TYPE_TEXT,
                false
        );

        ciDiscardAtomic = new ColumnInfo(
                BaseMessages.getString(PKG, "AwaitStepDialog.DiscardAtomic"),
                ColumnInfo.COLUMN_TYPE_CCOMBO,
                new String[] {"true", "false"}
        );

        ciTargetStep = new ColumnInfo(
                BaseMessages.getString(PKG, "AwaitStepDialog.TargetStep"),
                ColumnInfo.COLUMN_TYPE_CCOMBO,
                nextStepNames
        );

        final ColumnInfo[] awaitTableColumns = {
                ciAtomicValue,
                ciDiscardAtomic,
                ciTargetStep
        };

        wAwaitTableView = new TableView(
                transMeta, settingsGroup, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
                awaitTableColumns,1, lsFieldsModify, props);
        final FormData fdAwaitTableView = new FormDataBuilder().left()
                .top(wActionIfNoAtomicLabel, ELEMENT_SPACING)
                .fullWidth()
                .height(ELEMENT_SPACING * 10)
                .result();
        wAwaitTableView.setLayoutData(fdAwaitTableView);

        // Group for wait loop
        final Group waitLoopGroup = new Group(settingsGroup, SWT.SHADOW_ETCHED_IN);
        props.setLook(waitLoopGroup);
        waitLoopGroup.setText(BaseMessages.getString(PKG, "AwaitStepDialog.GroupText.WaitLoop"));
        final FormLayout waitLoopGroupLayout = new FormLayout();
        waitLoopGroupLayout.marginWidth = MARGIN_SIZE;
        waitLoopGroupLayout.marginHeight = MARGIN_SIZE;
        waitLoopGroup.setLayout(waitLoopGroupLayout);
        final FormData waitLoopGroupLayoutData = new FormDataBuilder().fullWidth()
                .top(wAwaitTableView, MARGIN_SIZE)
                .result();
        waitLoopGroup.setLayoutData(waitLoopGroupLayoutData);

        // check period label/field
        wWaitLoopCheckPeriodLabel = new Label(waitLoopGroup, SWT.LEFT);
        props.setLook(wWaitLoopCheckPeriodLabel);
        wWaitLoopCheckPeriodLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.TextFieldCheckPeriod"));
        final FormData fdWaitLoopCheckPeriodLabel = new FormDataBuilder().left()
                .top(wAwaitTableView, ELEMENT_SPACING)
                .result();
        wWaitLoopCheckPeriodLabel.setLayoutData(fdWaitLoopCheckPeriodLabel);

        wWaitLoopCheckPeriodField = new Text(waitLoopGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wWaitLoopCheckPeriodField);
        wWaitLoopCheckPeriodField.addModifyListener(lsFieldsModify);
        wWaitLoopCheckPeriodField.addVerifyListener(new NumberVerifyListener(Long::parseLong));
        final FormData fdWaitLoopCheckPeriodField = new FormDataBuilder().left(wWaitLoopCheckPeriodLabel, LABEL_SPACING)
                .top(wAwaitTableView, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wWaitLoopCheckPeriodField.setLayoutData(fdWaitLoopCheckPeriodField);

        // timeout label/field
        wWaitLoopTimeoutLabel = new Label(waitLoopGroup, SWT.LEFT);
        props.setLook(wWaitLoopTimeoutLabel);
        wWaitLoopTimeoutLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.TextFieldTimeout"));
        final FormData fdWaitLoopTimeoutLabel = new FormDataBuilder().left()
                .top(wWaitLoopCheckPeriodLabel, ELEMENT_SPACING)
                .result();
        wWaitLoopTimeoutLabel.setLayoutData(fdWaitLoopTimeoutLabel);

        wWaitLoopTimeoutField = new Text(waitLoopGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wWaitLoopTimeoutField);
        wWaitLoopTimeoutField.addModifyListener(lsFieldsModify);
        wWaitLoopTimeoutField.addVerifyListener(new NumberVerifyListener(Long::parseLong));
        final FormData fdWaitLoopTimeoutField = new FormDataBuilder().left(wWaitLoopTimeoutLabel, LABEL_SPACING)
                .top(wWaitLoopCheckPeriodLabel, ELEMENT_SPACING)
                .width(SMALL_FIELD)
                .result();
        wWaitLoopTimeoutField.setLayoutData(fdWaitLoopTimeoutField);

        // timeout target label/field
        wTimeoutTargetLabel = new Label(waitLoopGroup, SWT.LEFT);
        props.setLook(wTimeoutTargetLabel);
        wTimeoutTargetLabel.setText(BaseMessages.getString(PKG, "AwaitStepDialog.TextFieldTimeoutTarget"));
        final FormData fdTimeoutTargetLabel = new FormDataBuilder().left()
                .top(wWaitLoopTimeoutLabel, ELEMENT_SPACING)
                .result();
        wTimeoutTargetLabel.setLayoutData(fdTimeoutTargetLabel);

        wTimeoutTargetField = new CCombo(waitLoopGroup, SWT.DROP_DOWN | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wTimeoutTargetField.setItems(nextStepNames);
        props.setLook(wTimeoutTargetField);
        wTimeoutTargetField.addModifyListener(lsFieldsModify);
        final FormData fdTimeoutTargetField = new FormDataBuilder().left(wTimeoutTargetLabel, LABEL_SPACING)
                .top(wWaitLoopTimeoutLabel, ELEMENT_SPACING)
                .width(MEDIUM_FIELD)
                .result();
        wTimeoutTargetField.setLayoutData(fdTimeoutTargetField);

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

    private void getData(final AwaitStepMeta meta) {
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
        ciAtomicValue.setFieldTypeColumn(atomicType == AtomicType.Boolean ? ColumnInfo.COLUMN_TYPE_CCOMBO : ColumnInfo.COLUMN_TYPE_TEXT);

        final List<AwaitTarget> awaitValues = meta.getAwaitValues();
        if (awaitValues != null) {
            wAwaitTableView.getTable().removeAll();
            for (final AwaitTarget awaitValue : awaitValues) {
                // TODO(AR) fix atomic types?
                final String targetStepname = awaitValue.getTargetStep() != null ? awaitValue.getTargetStep().getName() : awaitValue.getTargetStepname();
                wAwaitTableView.add(new String[]  { strNullIfNull(nullIfEmpty(awaitValue.getAtomicValue())), Boolean.toString(awaitValue.isDiscardAtomic()),  emptyIfNull(targetStepname)});
            }
        }

        wWaitLoopCheckPeriodField.setText(Long.toString(meta.getWaitLoopCheckPeriod()));
        wWaitLoopTimeoutField.setText(Long.toString(meta.getWaitLoopTimeout()));

        wTimeoutTargetField.setText(meta.getTimeoutTargetStep() == null ? "" : meta.getTimeoutTargetStep().getName());
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

        final int awaitValuesLen = wAwaitTableView.getItemCount();
        final List<AwaitTarget> awaitValues = new ArrayList<>(awaitValuesLen);
        for (int i = 0; i < awaitValuesLen; i++) {
            String atomicValue = wAwaitTableView.getItem(i, 1);
            final String discardAtomic = wAwaitTableView.getItem(i, 2);
            if (atomicValue != null && discardAtomic != null) {
                try {
                    final StepMeta targetStep;
                    final String targetStepName = wAwaitTableView.getItem(i, 3);
                    if (!isNullOrEmpty(targetStepName)) {
                        targetStep = transMeta.findStep(targetStepName);
                    } else {
                        targetStep = null;
                    }

                    atomicValue = nullIfStrNull(nullIfEmpty(atomicValue));
                    if (atomicValue != null) {
                        atomicValue = atomicType.checkValidValue(atomicValue);
                    }

                    awaitValues.add(new AwaitTarget(atomicValue, Boolean.valueOf(discardAtomic), targetStep));
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
        meta.setAwaitValues(awaitValues);

        try {
            final long checkPeriod = Long.parseLong(wWaitLoopCheckPeriodField.getText());
            meta.setWaitLoopCheckPeriod(checkPeriod);
        } catch (final NumberFormatException e) {
            //TODO(AR) show an error to the user
            throw e;
        }

        try {
            final long timeout = Long.parseLong(wWaitLoopTimeoutField.getText());
            meta.setWaitLoopTimeout(timeout);
        } catch (final NumberFormatException e) {
            //TODO(AR) show an error to the user
            throw e;
        }

        final String timeoutTargetName = this.wTimeoutTargetField.getText();
        if (!isNullOrEmpty(timeoutTargetName)) {
            final StepMeta timeoutTargetStep = transMeta.findStep(timeoutTargetName);
            meta.setTimeoutTargetStep(timeoutTargetStep);
        } else {
            meta.setTimeoutTargetStep(null);
        }
    }
}
