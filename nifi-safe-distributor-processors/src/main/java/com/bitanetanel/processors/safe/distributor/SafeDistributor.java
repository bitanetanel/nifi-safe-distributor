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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;


import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * @author Netanel Bitan
 */
@SideEffectFree
@Tags({"distributor", "distribute"})
@CapabilityDescription("Safely distribute flowfiles between the out relationships. Flowfiles with the same" +
        "value in a selected attribute will always distribute to the same relationship.")
public class SafeDistributor extends AbstractProcessor {


    /* --- Relationships --- */

    /** Flow files will transfer to this relationship if they don't have the wanted attribute. */
    protected static final Relationship FAILURE = new Relationship.Builder().name("Failure").build();

    /** The desired attribute that we want to be based on for our safeness. */
    protected static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Attribute name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    /* --- Properties --- */

    /** The number of output relationships that we want to route to. */
    protected static final PropertyDescriptor RELATIONSHIPS_NUMBER = new PropertyDescriptor.Builder()
            .name("Relationships number")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build();


    /* --- Data Members --- */

    /** The processor's static relationships. */
    private final Set<Relationship> staticRelationships = Sets.newHashSet();

    /** The processor's dynamic relationships. Changed whenever {@link #RELATIONSHIPS_NUMBER} changes. */
    private final Map<String, Relationship> dynamicRelationships = Maps.newHashMap();

    /** The processor's properties. */
    private final List<PropertyDescriptor> properties = Lists.newArrayList();


    /* --- Override Methods --- */

    /**
     * Initializes relationships and properties.
     *
     * @param context The processor's context.
     */
    @Override
    protected void init(ProcessorInitializationContext context) {
        dynamicRelationships.put("1", createRelationship(1));
        staticRelationships.add(FAILURE);
        properties.add(ATTRIBUTE_NAME);
        properties.add(RELATIONSHIPS_NUMBER);
    }

    /**
     * Get all relationships of the processor.
     * Merges the built-in relationships (such as {@link #FAILURE} and the dynamic created relationships.
     *
     * @return All relationships of the processor.
     */
    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = Sets.newHashSet();
        relationships.addAll(staticRelationships);
        relationships.addAll(dynamicRelationships.values());
        return relationships;
    }

    /**
     * @return All properties of the processor.
     */
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Creates the {@link #dynamicRelationships} according to the value at {@link #RELATIONSHIPS_NUMBER}.
     *
     * @param descriptor The changed property.
     * @param oldValue Old value of the property.
     * @param newValue New value of the property.
     */
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.equals(RELATIONSHIPS_NUMBER)) {
            dynamicRelationships.clear();
            IntStream.rangeClosed(1, Integer.parseInt(newValue))
                    .forEach(number -> dynamicRelationships.put(String.valueOf(number), createRelationship(number)));
        }
    }


    /* --- AbstractProcessor Implementation --- */

    /**
     * Gets a flowfile and distribute it to one of the {@link #dynamicRelationships}.
     * Always transfer flowfiles with the same value in the selected {@link #ATTRIBUTE_NAME}.
     *
     * @param processContext The context of the process
     * @param processSession The current process session.
     */
    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) {
        FlowFile flowFile = processSession.get();

        if (flowFile == null) {
            return;
        }

        String attributeName = processContext.getProperty(ATTRIBUTE_NAME).getValue();
        String attributeValue = flowFile.getAttribute(attributeName);

        if (attributeValue == null) {
            getLogger().warn(String.format("Attribute '%s' wasn't found in the flow file.", attributeName));
            processSession.transfer(flowFile, FAILURE);
            return;
        }

        processSession.transfer(flowFile, calculatedDestination(attributeValue));
    }


    /* --- Private Methods --- */

    /**
     * Calculates the relationship destination of the flowfile according to the given attribute value.
     *
     * @param attributeValue The attribute value.
     * @return The calculated destination relationship.
     */
    private Relationship calculatedDestination(String attributeValue) {
        int hash = Hashing.murmur3_32().hashBytes(attributeValue.getBytes()).asInt();
        int destinationNumber = Math.floorMod(hash, dynamicRelationships.size()) + 1;
        return dynamicRelationships.get(String.valueOf(destinationNumber));
    }

    /**
     * Creates and returns a new dynamic relationship of the given number.
     *
     * @param number The number of the new dynamic relationship.
     * @return The new created relationship.
     */
    private Relationship createRelationship(int number) {
        return new Relationship.Builder().name(String.valueOf(number)).build();
    }
}
