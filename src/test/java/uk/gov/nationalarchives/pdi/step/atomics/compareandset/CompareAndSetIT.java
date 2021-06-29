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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class CompareAndSetIT {

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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Initialise);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setInitialiseAtomicValue(initialiseValue);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Initialise);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setInitialiseAtomicValue(initialiseValue);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        compareAndSetStepMeta.setAtomicType(atomicType);

        final TransMeta transMeta = TransTestFactory.generateTestTransformationError(new Variables(), compareAndSetStepMeta, stepName);
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Continue);
        compareAndSetStepMeta.setContinueTargetStepname(TransTestFactory.DUMMY_STEPNAME);
        compareAndSetStepMeta.setAtomicType(atomicType);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Wait);
        compareAndSetStepMeta.setWaitAtomicTimeout(5000);  // a suitably long time to enable us to set it
        compareAndSetStepMeta.setWaitAtomicCheckPeriod(50);

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

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);

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
            assertEquals(atomicType, atomicValue.getType());
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Wait);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setWaitAtomicTimeout(200);
        compareAndSetStepMeta.setWaitAtomicTimeout(50);

        final TransMeta transMeta = TransTestFactory.generateTestTransformationError(new Variables(), compareAndSetStepMeta, stepName);
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
    private static Stream<Arguments> initialiseAndCasArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer,
                        "1",
                        Arrays.asList(
                                new CompareAndSetTarget("0", "100", TransTestFactory.DUMMY_STEPNAME),
                                new CompareAndSetTarget("1", "2", TransTestFactory.DUMMY_STEPNAME),
                                new CompareAndSetTarget("2", "3", TransTestFactory.DUMMY_STEPNAME)
                        ),
                        "2"
                ),
                Arguments.of(
                        AtomicType.Boolean,
                        "true",
                        Arrays.asList(
                                new CompareAndSetTarget("false", "true", TransTestFactory.DUMMY_STEPNAME),
                                new CompareAndSetTarget("true", "false", TransTestFactory.DUMMY_STEPNAME)
                        ),
                        "false"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("initialiseAndCasArgs")
    public void initialiseAndCas(final AtomicType atomicType, final String initialiseValue, final List<CompareAndSetTarget> compareAndSetValues, final String expectedValue) throws KettleException {
        final String stepName = "initialiseAndCas";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Initialise);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setInitialiseAtomicValue(initialiseValue);

        compareAndSetStepMeta.setCompareAndSetValues(compareAndSetValues);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);
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
            assertEquals(Integer.valueOf(expectedValue), ((AtomicIntegerValue) atomicValue).get());
        } else {
            assertTrue(atomicValue instanceof AtomicBooleanValue);
            assertEquals(Boolean.valueOf(expectedValue), ((AtomicBooleanValue) atomicValue).get());
        }
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> casArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer,
                        "2",
                        Arrays.asList(
                                new CompareAndSetTarget("0", "100", TransTestFactory.DUMMY_STEPNAME),
                                new CompareAndSetTarget("1", "2", TransTestFactory.DUMMY_STEPNAME),
                                new CompareAndSetTarget("2", "3", TransTestFactory.DUMMY_STEPNAME)
                        ),
                        "3"
                ),
                Arguments.of(
                        AtomicType.Boolean,
                        "false",
                        Arrays.asList(
                                new CompareAndSetTarget("false", "true", TransTestFactory.DUMMY_STEPNAME),
                                new CompareAndSetTarget("true", "false", TransTestFactory.DUMMY_STEPNAME)
                        ),
                        "true"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("casArgs")
    public void cas(final AtomicType atomicType, final String existingAtomicValue, final List<CompareAndSetTarget> compareAndSetValues, final String expectedValue) throws KettleException {
        final String stepName = "cas";
        final String atomicIdFieldName = "atomicIdField";
        final String atomicIdFieldValue = "atomicId1";

        // prepare the storage
        final AtomicValue atomicValue;
        if (atomicType == AtomicType.Integer) {
            atomicValue = new AtomicIntegerValue(Integer.parseInt(existingAtomicValue));
        } else {
            atomicValue = new AtomicBooleanValue(Boolean.parseBoolean(existingAtomicValue));
        }
        AtomicStorageTestHelper.set(atomicIdFieldValue, atomicValue);

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        compareAndSetStepMeta.setAtomicType(atomicType);

        compareAndSetStepMeta.setCompareAndSetValues(compareAndSetValues);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);
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
            assertEquals(Integer.valueOf(expectedValue), ((AtomicIntegerValue) atomicValueResult).get());
        } else {
            assertTrue(atomicValueResult instanceof AtomicBooleanValue);
            assertEquals(Boolean.valueOf(expectedValue), ((AtomicBooleanValue) atomicValueResult).get());
        }
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> casSkipArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer, "1", Collections.singletonList(new CompareAndSetTarget("99", "100", TransTestFactory.DUMMY_STEPNAME))
                ),
                Arguments.of(
                        AtomicType.Boolean, "true", Collections.singletonList(new CompareAndSetTarget("false", "true", TransTestFactory.DUMMY_STEPNAME))
                )
        );
    }

    @ParameterizedTest
    @MethodSource("casSkipArgs")
    public void casSkip(final AtomicType atomicType, final String existingAtomicValue, final List<CompareAndSetTarget> compareAndSetValues) throws KettleException {
        final String stepName = "casSkip";
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setActionIfUnableToSet(ActionIfUnableToSet.Skip);
        compareAndSetStepMeta.setSkipTargetStepname(TransTestFactory.DUMMY_STEPNAME);

        compareAndSetStepMeta.setCompareAndSetValues(compareAndSetValues);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);
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

    @SuppressWarnings("unused")
    private static Stream<Arguments> casErrorArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer, "1", Collections.singletonList(new CompareAndSetTarget("99", "100", TransTestFactory.DUMMY_STEPNAME))
                ),
                Arguments.of(
                        AtomicType.Boolean, "true", Collections.singletonList(new CompareAndSetTarget("false", "true", TransTestFactory.DUMMY_STEPNAME))
                )
        );
    }

    @ParameterizedTest
    @MethodSource("casErrorArgs")
    public void casError(final AtomicType atomicType, final String existingAtomicValue, final List<CompareAndSetTarget> compareAndSetValues) throws KettleException {
        final String stepName = "casError";
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setActionIfUnableToSet(ActionIfUnableToSet.Error);

        compareAndSetStepMeta.setCompareAndSetValues(compareAndSetValues);

        final TransMeta transMeta = TransTestFactory.generateTestTransformationError(new Variables(), compareAndSetStepMeta, stepName);
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
    private static Stream<Arguments> casLoopArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer, "1", "2", Collections.singletonList(new CompareAndSetTarget("2", "4", TransTestFactory.DUMMY_STEPNAME)), "4"
                ),
                Arguments.of(
                        AtomicType.Boolean, "true", "false", Collections.singletonList(new CompareAndSetTarget("false", "true", TransTestFactory.DUMMY_STEPNAME)), "true"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("casLoopArgs")
    public void casLoop(final AtomicType atomicType, final String existingAtomicValue, final String updatedAtomicValue, final List<CompareAndSetTarget> compareAndSetValues, final String expectedValue) throws KettleException, InterruptedException {
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setActionIfUnableToSet(ActionIfUnableToSet.Loop);
        compareAndSetStepMeta.setUnableToSetLoopTimeout(5000);  // a suitably long time to enable us to set it
        compareAndSetStepMeta.setUnableToSetLoopCheckPeriod(50);

        compareAndSetStepMeta.setCompareAndSetValues(compareAndSetValues);

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

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);

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
                assertEquals(Integer.valueOf(expectedValue), ((AtomicIntegerValue) atomicValueResult).get());
            } else {
                assertTrue(atomicValueResult instanceof AtomicBooleanValue);
                assertEquals(Boolean.valueOf(expectedValue), ((AtomicBooleanValue) atomicValueResult).get());
            }
        } finally {
            setAtomicThread.join();
        }
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> casLoopTimeoutArgs() {
        return Stream.of(
                Arguments.of(
                        AtomicType.Integer, "1", Collections.singletonList(new CompareAndSetTarget("2", "4", TransTestFactory.DUMMY_STEPNAME))
                ),
                Arguments.of(
                        AtomicType.Boolean, "true", Collections.singletonList(new CompareAndSetTarget("false", "true", TransTestFactory.DUMMY_STEPNAME))
                )
        );
    }

    @ParameterizedTest
    @MethodSource("casLoopTimeoutArgs")
    public void casLoopTimeout(final AtomicType atomicType, final String existingAtomicValue, final List<CompareAndSetTarget> compareAndSetValues) throws KettleException {
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

        final CompareAndSetStepMeta compareAndSetStepMeta = new CompareAndSetStepMeta();
        compareAndSetStepMeta.setAtomicIdFieldName(atomicIdFieldName);
        compareAndSetStepMeta.setActionIfNoAtomic(ActionIfNoAtomic.Error);
        compareAndSetStepMeta.setAtomicType(atomicType);
        compareAndSetStepMeta.setActionIfUnableToSet(ActionIfUnableToSet.Loop);
        compareAndSetStepMeta.setUnableToSetLoopTimeout(200);
        compareAndSetStepMeta.setUnableToSetLoopCheckPeriod(50);
        compareAndSetStepMeta.setTimeoutTargetStepname(TransTestFactory.DUMMY_STEPNAME);

        compareAndSetStepMeta.setCompareAndSetValues(compareAndSetValues);

        final TransMeta transMeta = TransTestFactory.generateTestTransformation(new Variables(), compareAndSetStepMeta, stepName);
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
