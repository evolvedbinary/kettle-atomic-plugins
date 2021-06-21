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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.RowStepCollector;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransTestFactory;
import uk.gov.nationalarchives.pdi.step.atomics.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AwaitIT {

    @BeforeAll
    public static void setup() throws KettleException {
        KettleClientEnvironment.init();
    }

    @AfterEach
    public void resetStorage() {
        AtomicStorageTestHelper.clear();
    }
    @SuppressWarnings("unused")
    private static Stream<Arguments> initialiseIfNoSuchAtomicArgs() {
        return Stream.of(
                Arguments.of(AtomicType.Integer, "1"),
                Arguments.of(AtomicType.Boolean, "true")
        );
    }

    @ParameterizedTest
    @MethodSource("initialiseIfNoSuchAtomicArgs")
    public void initialiseIfNoSuchAtomic(final AtomicType atomicType, final String initialiseValue) throws KettleException {
        final String stepName = "initialiseIfNoSuchAtomic";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Initialise);
        awaitStepMeta.setAtomicType(atomicType);
        awaitStepMeta.setInitialiseAtomicValue(initialiseValue);
        awaitStepMeta.setAtomicValue(initialiseValue);
        awaitStepMeta.setAtomicValueTargetStepname(TransTestFactory.DUMMY_STEPNAME);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), awaitStepMeta, stepName);
        final List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(
                transMeta,
                TransTestFactory.INJECTOR_STEPNAME,
                stepName,
                TransTestFactory.DUMMY_STEPNAME,
                generateInputData(atomicIdFieldName, atomicIdFieldValue));
        assertEquals(1, result.size());

        final Map<String, AtomicValue> stored = AtomicStorageTestHelper.copy();
        assertEquals(1, stored.size());
        final AtomicValue atomicValue = stored.get(atomicIdFieldValue);
        assertNotNull(atomicValue);
        assertEquals(atomicType, atomicValue.getType());
        if (atomicType == AtomicType.Integer) {
            assertTrue(atomicValue instanceof AtomicIntegerValue);
            assertEquals(Integer.valueOf(initialiseValue), ((AtomicIntegerValue) atomicValue).get());
        } else {
            assertTrue(atomicValue instanceof AtomicBooleanValue);
            assertEquals(Boolean.valueOf(initialiseValue), ((AtomicBooleanValue) atomicValue).get());
        }
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> initialiseIfNoSuchAtomicExistingAtomicArgs() {
        return Stream.of(
                Arguments.of(AtomicType.Integer, "1", "2"),
                Arguments.of(AtomicType.Boolean, "false", "true")
        );
    }

    @ParameterizedTest
    @MethodSource("initialiseIfNoSuchAtomicExistingAtomicArgs")
    public void initialiseIfNoSuchAtomicExistingAtomic(final AtomicType atomicType, final String initialiseValue, final String existingAtomicValue) throws KettleException {
        final String stepName = "initialiseIfNoSuchAtomicExistingAtomic";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        // prepare the storage
        if (atomicType == AtomicType.Integer) {
            AtomicStorageTestHelper.set(atomicIdFieldValue, new AtomicIntegerValue(Integer.parseInt(existingAtomicValue)));
        } else {
            AtomicStorageTestHelper.set(atomicIdFieldValue, new AtomicBooleanValue(Boolean.parseBoolean(existingAtomicValue)));
        }

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Initialise);
        awaitStepMeta.setAtomicType(atomicType);
        awaitStepMeta.setInitialiseAtomicValue(initialiseValue);
        awaitStepMeta.setAtomicValue(existingAtomicValue);
        awaitStepMeta.setAtomicValueTargetStepname(TransTestFactory.DUMMY_STEPNAME);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), awaitStepMeta, stepName);
        final List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(
                transMeta,
                TransTestFactory.INJECTOR_STEPNAME,
                stepName,
                TransTestFactory.DUMMY_STEPNAME,
                generateInputData(atomicIdFieldName, atomicIdFieldValue));
        assertEquals(1, result.size());

        final Map<String, AtomicValue> stored = AtomicStorageTestHelper.copy();
        assertEquals(1, stored.size());
        final AtomicValue atomicValue = stored.get(atomicIdFieldValue);
        assertNotNull(atomicValue);
        assertEquals(atomicType, atomicValue.getType());
        if (atomicType == AtomicType.Integer) {
            assertTrue(atomicValue instanceof AtomicIntegerValue);
            assertEquals(Integer.valueOf(existingAtomicValue), ((AtomicIntegerValue) atomicValue).get());
        } else {
            assertTrue(atomicValue instanceof AtomicBooleanValue);
            assertEquals(Boolean.valueOf(existingAtomicValue), ((AtomicBooleanValue) atomicValue).get());
        }
    }

    @ParameterizedTest
    @EnumSource(AtomicType.class)
    public void errorIfNoSuchAtomic(final AtomicType atomicType) throws KettleException {
        final String stepName = "errorIfNoSuchAtomic";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        awaitStepMeta.setAtomicType(atomicType);

        final TransMeta transMeta = TransTestFactory.generateTestTransformationError(new Variables(), awaitStepMeta, stepName);
        final Map<String, RowStepCollector> result = TransTestFactory.executeTestTransformationError(
                transMeta,
                TransTestFactory.INJECTOR_STEPNAME,
                stepName,
                TransTestFactory.DUMMY_STEPNAME,
                TransTestFactory.ERROR_STEPNAME,
                generateInputData(atomicIdFieldName, atomicIdFieldValue));

        assertEquals(3, result.size());

        final RowStepCollector casStepCollector = result.get(stepName);
        assertEquals(1, casStepCollector.getRowsRead().size());
        assertEquals(0, casStepCollector.getRowsWritten().size());
        assertEquals(1, casStepCollector.getRowsError().size());

        final RowStepCollector dummyStepCollector = result.get(TransTestFactory.DUMMY_STEPNAME);
        assertEquals(0, dummyStepCollector.getRowsRead().size());
        assertEquals(0, dummyStepCollector.getRowsWritten().size());
        assertEquals(0, dummyStepCollector.getRowsError().size());

        final RowStepCollector errorStepCollector = result.get(TransTestFactory.ERROR_STEPNAME);
        assertEquals(1, errorStepCollector.getRowsRead().size());
        assertEquals(1, errorStepCollector.getRowsWritten().size());
        assertEquals(0, errorStepCollector.getRowsError().size());
    }

    @ParameterizedTest
    @EnumSource(AtomicType.class)
    public void continueIfNoSuchAtomic(final AtomicType atomicType) throws KettleException {
        final String stepName = "continueIfNoSuchAtomic";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Continue);
        awaitStepMeta.setContinueTargetStepname(TransTestFactory.DUMMY_STEPNAME);
        awaitStepMeta.setAtomicType(atomicType);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), awaitStepMeta, stepName);
        final List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(
                transMeta,
                TransTestFactory.INJECTOR_STEPNAME,
                stepName,
                TransTestFactory.DUMMY_STEPNAME,
                generateInputData(atomicIdFieldName, atomicIdFieldValue));
        assertEquals(1, result.size());

        final Map<String, AtomicValue> stored = AtomicStorageTestHelper.copy();
        assertTrue(stored.isEmpty());
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> waitIfNoSuchAtomicArgs() {
        return Stream.of(
                Arguments.of(AtomicType.Integer, "1"),
                Arguments.of(AtomicType.Boolean, "true")
        );
    }

    @ParameterizedTest
    @MethodSource("waitIfNoSuchAtomicArgs")
    public void waitIfNoSuchAtomic(final AtomicType atomicType, final String waitForValue) throws KettleException, InterruptedException {
        final String stepName = "waitIfNoSuchAtomic";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setAtomicType(atomicType);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Wait);
        awaitStepMeta.setWaitAtomicTimeout(5000);  // a suitably long time to enable us to set it
        awaitStepMeta.setWaitAtomicCheckPeriod(50);
        awaitStepMeta.setAtomicValue(waitForValue);
        awaitStepMeta.setAtomicValueTargetStepname(TransTestFactory.DUMMY_STEPNAME);

        final AtomicValue atomicValue;
        if (atomicType == AtomicType.Integer) {
            atomicValue = new AtomicIntegerValue(Integer.parseInt(waitForValue));
        } else {
            atomicValue = new AtomicBooleanValue(Boolean.parseBoolean(waitForValue));
        }

        final Thread setAtomicThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();  // restore interrupted flag
            }
            AtomicStorageTestHelper.set(atomicIdFieldValue, atomicValue);
        });

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), awaitStepMeta, stepName);

        setAtomicThread.start();
        try {
            final List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(
                    transMeta,
                    TransTestFactory.INJECTOR_STEPNAME,
                    stepName,
                    TransTestFactory.DUMMY_STEPNAME,
                    generateInputData(atomicIdFieldName, atomicIdFieldValue));
            assertEquals(1, result.size());

            final Map<String, AtomicValue> stored = AtomicStorageTestHelper.copy();
            assertEquals(1, stored.size());
            final AtomicValue atomicValueResult = stored.get(atomicIdFieldValue);
            assertNotNull(atomicValueResult);
            assertEquals(atomicType, atomicValueResult.getType());
            if (atomicType == AtomicType.Integer) {
                assertTrue(atomicValueResult instanceof AtomicIntegerValue);
                assertEquals(Integer.valueOf(waitForValue), ((AtomicIntegerValue) atomicValueResult).get());
            } else {
                assertTrue(atomicValueResult instanceof AtomicBooleanValue);
                assertEquals(Boolean.valueOf(waitForValue), ((AtomicBooleanValue) atomicValueResult).get());
            }

        } finally {
            setAtomicThread.join();
        }
    }

    @ParameterizedTest
    @EnumSource(AtomicType.class)
    public void waitIfNoSuchAtomicTimeout(final AtomicType atomicType) throws KettleException {
        final String stepName = "waitIfNoSuchAtomicTimeout";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Wait);
        awaitStepMeta.setAtomicType(atomicType);
        awaitStepMeta.setWaitAtomicTimeout(200);
        awaitStepMeta.setWaitAtomicTimeout(50);

        final TransMeta transMeta = TransTestFactory.generateTestTransformationError(new Variables(), awaitStepMeta, stepName);
        final Map<String, RowStepCollector> result = TransTestFactory.executeTestTransformationError(
                transMeta,
                TransTestFactory.INJECTOR_STEPNAME,
                stepName,
                TransTestFactory.DUMMY_STEPNAME,
                TransTestFactory.ERROR_STEPNAME,
                generateInputData(atomicIdFieldName, atomicIdFieldValue));

        assertEquals(3, result.size());

        final RowStepCollector casStepCollector = result.get(stepName);
        assertEquals(1, casStepCollector.getRowsRead().size());
        assertEquals(0, casStepCollector.getRowsWritten().size());
        assertEquals(1, casStepCollector.getRowsError().size());

        final RowStepCollector dummyStepCollector = result.get(TransTestFactory.DUMMY_STEPNAME);
        assertEquals(0, dummyStepCollector.getRowsRead().size());
        assertEquals(0, dummyStepCollector.getRowsWritten().size());
        assertEquals(0, dummyStepCollector.getRowsError().size());

        final RowStepCollector errorStepCollector = result.get(TransTestFactory.ERROR_STEPNAME);
        assertEquals(1, errorStepCollector.getRowsRead().size());
        assertEquals(1, errorStepCollector.getRowsWritten().size());
        assertEquals(0, errorStepCollector.getRowsError().size());
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> awaitLoopArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer, "1", "2"
                ),
                Arguments.of(
                        AtomicType.Boolean, "true", "false"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("awaitLoopArgs")
    public void awaitLoop(final AtomicType atomicType, final String existingAtomicValue, final String updatedAtomicValue) throws KettleException, InterruptedException {
        final String stepName = "casLoop";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        // prepare the storage
        final AtomicValue atomicValue;
        if (atomicType ==  AtomicType.Integer) {
            atomicValue = new AtomicIntegerValue(Integer.parseInt(existingAtomicValue));
        } else {
            atomicValue = new AtomicBooleanValue(Boolean.parseBoolean(existingAtomicValue));
        }
        AtomicStorageTestHelper.set(atomicIdFieldValue, atomicValue);

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        awaitStepMeta.setAtomicType(atomicType);
        awaitStepMeta.setWaitLoopTimeout(5000);  // a suitably long time to enable us to set it
        awaitStepMeta.setWaitLoopCheckPeriod(50);
        awaitStepMeta.setAtomicValue(updatedAtomicValue);
        awaitStepMeta.setAtomicValueTargetStepname(TransTestFactory.DUMMY_STEPNAME);

        final AtomicValue atomicValue2;
        if (atomicType == AtomicType.Integer) {
            atomicValue2 = new AtomicIntegerValue(Integer.parseInt(updatedAtomicValue));
        } else {
            atomicValue2 = new AtomicBooleanValue(Boolean.parseBoolean(updatedAtomicValue));
        }
        final Thread setAtomicThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();  // restore interrupted flag
            }
            AtomicStorageTestHelper.put(atomicIdFieldValue, atomicValue2);
        });

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), awaitStepMeta, stepName);

        setAtomicThread.start();
        try {
            final List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(
                    transMeta,
                    TransTestFactory.INJECTOR_STEPNAME,
                    stepName,
                    TransTestFactory.DUMMY_STEPNAME,
                    generateInputData(atomicIdFieldName, atomicIdFieldValue));
            assertEquals(1, result.size());

            final Map<String, AtomicValue> stored = AtomicStorageTestHelper.copy();
            assertEquals(1, stored.size());
            final AtomicValue atomicValueResult = stored.get(atomicIdFieldValue);
            assertNotNull(atomicValueResult);
            assertEquals(atomicType, atomicValueResult.getType());
            if (atomicType == AtomicType.Integer) {
                assertTrue(atomicValueResult instanceof AtomicIntegerValue);
                assertEquals(Integer.valueOf(updatedAtomicValue), ((AtomicIntegerValue) atomicValueResult).get());
            } else {
                assertTrue(atomicValueResult instanceof AtomicBooleanValue);
                assertEquals(Boolean.valueOf(updatedAtomicValue), ((AtomicBooleanValue) atomicValueResult).get());
            }
        } finally {
            setAtomicThread.join();
        }
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> awaitLoopTimeoutArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer, "1", "2"
                ),
                Arguments.of(
                        AtomicType.Boolean, "true", "false"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("awaitLoopTimeoutArgs")
    public void awaitLoopTimeout(final AtomicType atomicType, final String existingAtomicValue, final String awaitAtomicValue) throws KettleException {
        final String stepName = "casLoopTimeout";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        // prepare the storage
        final AtomicValue atomicValue;
        if (atomicType ==  AtomicType.Integer) {
            atomicValue = new AtomicIntegerValue(Integer.parseInt(existingAtomicValue));
        } else {
            atomicValue = new AtomicBooleanValue(Boolean.parseBoolean(existingAtomicValue));
        }
        AtomicStorageTestHelper.set(atomicIdFieldValue, atomicValue);

        final AwaitStepMeta awaitStepMeta = new AwaitStepMeta();
        awaitStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        awaitStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        awaitStepMeta.setAtomicType(atomicType);
        awaitStepMeta.setWaitLoopTimeout(200);
        awaitStepMeta.setWaitAtomicCheckPeriod(50);
        awaitStepMeta.setAtomicValue(awaitAtomicValue);
//        awaitStepMeta.setAtomicValueTargetStepname(TransTestFactory.DUMMY_STEPNAME);
        awaitStepMeta.setTimeoutTargetStepname(TransTestFactory.DUMMY_STEPNAME);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), awaitStepMeta, stepName);
        final List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(
                transMeta,
                TransTestFactory.INJECTOR_STEPNAME,
                stepName,
                TransTestFactory.DUMMY_STEPNAME,
                generateInputData(atomicIdFieldName, atomicIdFieldValue));
        assertEquals(1, result.size());

        final Map<String, AtomicValue> stored = AtomicStorageTestHelper.copy();
        assertEquals(1, stored.size());
        final AtomicValue atomicValueResult = stored.get(atomicIdFieldValue);
        assertNotNull(atomicValueResult);
        assertEquals(atomicType, atomicValueResult.getType());
        if (atomicType == AtomicType.Integer) {
            assertTrue(atomicValueResult instanceof AtomicIntegerValue);
            assertEquals(Integer.valueOf(existingAtomicValue), ((AtomicIntegerValue) atomicValueResult).get());
        } else {
            assertTrue(atomicValueResult instanceof AtomicBooleanValue);
            assertEquals(Boolean.valueOf(existingAtomicValue), ((AtomicBooleanValue) atomicValueResult).get());
        }
    }

    private List<RowMetaAndData> generateInputData(final String atomicIdFieldName, final String atomicIdFieldValue) {
        final RowMeta rowMeta = new RowMeta();
        rowMeta.addValueMeta(new ValueMetaString(atomicIdFieldName));
        return Arrays.asList(new RowMetaAndData(rowMeta, new Object[] { atomicIdFieldValue }));
    }
}
