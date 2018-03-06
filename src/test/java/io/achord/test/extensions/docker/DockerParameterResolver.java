/*
 * Copyright 2018 Mangelion
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

package io.achord.test.extensions.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Camelion
 * @since 16.01.2018
 */
final class DockerParameterResolver implements ParameterResolver {
    static final Set<Class> SUPPORTED_PARAMETERS;

    static {
        SUPPORTED_PARAMETERS = new HashSet<>();

        SUPPORTED_PARAMETERS.add(NetworkSettings.class);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return DockerExtension.getContainerId(extensionContext) != null
                && SUPPORTED_PARAMETERS.contains(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context) throws ParameterResolutionException {
        DockerClient dockerClient = DockerExtension.getDockerClient(context);

        if (NetworkSettings.class.equals(parameterContext.getParameter().getType())) {
            InspectContainerResponse response = dockerClient.inspectContainerCmd(DockerExtension.getContainerId(context)).exec();
            return response.getNetworkSettings();
        }

        return null;
    }
}
