/*
 * Copyright $year [Satish Iyengar]
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

package org.rassee.omniture.pig;

/**
 * Created by siyengar on 4/20/16.
 */
public class RowParsingException extends Exception {

    public RowParsingException(Throwable e, int expectedTabs, int foundTabs) {
        super(String.format("Error while parsing row found %n, expected %n - %s", foundTabs, expectedTabs, e.getMessage()), e);
    }

}
