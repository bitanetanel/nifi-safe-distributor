/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bitanetanel.processors.safe.distributor;

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SafeDistributor}.
 *
 * @author Netanel Bitan
 */
public class SafeDistributorTest {


    /* --- Constants --- */

    /** The key of the attribute to set on the attribute name property */
    private final String SOME_ATTRIBUTE_KEY = "Some attribute key";

    /** Some basic value for the attribute */
    private final String SOME_ATTRIBUTE_VALUE = "Some attribute value";


    /* --- Data Members ---*/

    /** NiFi's test runner */
    private TestRunner testRunner;


    /* --- Setup --- */

    @Before
    public void setup() {
        testRunner = TestRunners.newTestRunner(SafeDistributor.class);
        testRunner.setProperty(SafeDistributor.ATTRIBUTE_NAME, SOME_ATTRIBUTE_KEY);
    }


    /* --- Tests --- */

    @Test
    public void shouldTransferToTheOnlyRelationship() {
        testRunner.enqueue("Some content", ImmutableMap.of(SOME_ATTRIBUTE_KEY, SOME_ATTRIBUTE_VALUE));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred("1", 1);
    }

    @Test
    public void shouldFailIfAttributeNotFound() {
        testRunner.enqueue("Some content", ImmutableMap.of("Other attribute name", SOME_ATTRIBUTE_VALUE));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(SafeDistributor.FAILURE, 1);
    }

    @Test
    public void shouldTransferFilesWithDifferentHashToDifferentRelationships() {
        testRunner.setProperty(SafeDistributor.RELATIONSHIPS_NUMBER, "2");
        testRunner.enqueue("Some content", ImmutableMap.of(SOME_ATTRIBUTE_KEY, SOME_ATTRIBUTE_VALUE));
        testRunner.enqueue("Some content", ImmutableMap.of(SOME_ATTRIBUTE_KEY, "Value with other hash"));
        testRunner.run(2);
        testRunner.assertTransferCount("1", 1);
        testRunner.assertTransferCount("2", 1);
    }

    @Test
    public void shouldTransferFilesWithSameHashToSameRelationships() {
        testRunner.setProperty(SafeDistributor.RELATIONSHIPS_NUMBER, "2");
        testRunner.enqueue("Some content", ImmutableMap.of(SOME_ATTRIBUTE_KEY, SOME_ATTRIBUTE_VALUE));
        testRunner.enqueue("Some content", ImmutableMap.of(SOME_ATTRIBUTE_KEY, SOME_ATTRIBUTE_VALUE));
        testRunner.enqueue("Some content", ImmutableMap.of(SOME_ATTRIBUTE_KEY, "Value with other hash"));
        testRunner.run(3);
        testRunner.assertTransferCount("1", 2);
        testRunner.assertTransferCount("2", 1);
    }
}