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

package com.github.mangelion.test.extensions.docker;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Camelion
 * @since 16.01.2018
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(DockerExtension.class)
@ExtendWith(DockerParameterResolver.class)
public @interface DockerContainer {
    String image();

    String tag() default "latest";

    /**
     * PortBindings in one of supported format
     *
     * @return ports that should be bind from container to host
     * @see com.github.dockerjava.api.model.ExposedPort#parse(String)
     */
    String[] ports() default {};

    String[] arguments() default {};

    String net() default "bridge";
}
