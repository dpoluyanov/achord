/*
 * Copyright 2017-2018 Mangelion
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mangelion.test.extensions.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.PullImageResultCallback;
import org.junit.jupiter.api.extension.*;
import org.junit.platform.commons.support.AnnotationSupport;

import java.util.Arrays;

import static java.util.stream.Collectors.toList;

/**
 * @author Camelion
 * @since 16.01.2018
 */
public final class DockerExtension implements BeforeAllCallback, BeforeTestExecutionCallback, AfterTestExecutionCallback, AfterAllCallback {

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        findAnnotationAndStartContainer(context);
    }

    private void findAnnotationAndStartContainer(ExtensionContext context) {
        context.getElement()
                .flatMap(annotatedElement -> AnnotationSupport.findAnnotation(annotatedElement, DockerContainer.class))
                .map(descr -> startContainer(context, descr))
                .ifPresent(containerId -> getStore(context).put("containerId", containerId));
    }

    private String startContainer(ExtensionContext context, DockerContainer description) {
        // always pull fresh image
        getDockerClient(context).pullImageCmd(description.image())
                .withTag(description.tag())
                .exec(new PullImageResultCallback()).awaitSuccess();

        String containerId = createContainer(context, description);

        context.publishReportEntry("docker-container-id", containerId);

        getDockerClient(context).startContainerCmd(containerId).exec();

        return containerId;
    }

    private static ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(ExtensionContext.Namespace.create(DockerExtension.class, context.getUniqueId()));
    }

    static DockerClient getDockerClient(ExtensionContext context) {
        return context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL)
                .getOrComputeIfAbsent(
                        "DockerClient",
                        key -> DockerClientBuilder.getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder().build()).build(),
                        DockerClient.class);
    }

    private String createContainer(ExtensionContext context, DockerContainer description) {
        CreateContainerCmd containerCmd = getDockerClient(context).createContainerCmd(description.image());

        if (description.ports().length > 0) {
            String[] ports = description.ports();

            containerCmd = containerCmd.withPortBindings(Arrays.stream(ports)
                    .map(PortBinding::parse)
                    .collect(toList()));
        }

        if (!description.net().equals("bridge")) {
            containerCmd = containerCmd.withNetworkMode(description.net());
        }

        if (description.arguments().length > 0) {
            containerCmd = containerCmd.withCmd(description.arguments());
        }

        CreateContainerResponse response = containerCmd.exec();

        return response.getId();
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        stopContainer(context);
    }

    private void stopContainer(ExtensionContext context) {
        String containerId = getContainerId(context);
        if (containerId != null) {
            getDockerClient(context).removeContainerCmd(containerId)
                    .withRemoveVolumes(true)
                    .withForce(true).exec();
        }
    }

    static String getContainerId(ExtensionContext context) {
        return getStore(context).get("containerId", String.class);
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        findAnnotationAndStartContainer(context);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        stopContainer(context);
    }
}
