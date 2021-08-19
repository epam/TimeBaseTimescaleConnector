/*
 * Copyright 2021 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.timebase.connector.util;

public class DiscoveryUtils {

    private static final String ASTERISK_CHAR = "*";
    private static final String QUESTION_MARK_CHAR = "?";
    private static final String DOT_CHAR = ".";

    public static boolean isExpression(String text) {
        if (text.contains(ASTERISK_CHAR) || text.contains(QUESTION_MARK_CHAR) || text.contains(DOT_CHAR)) {
            return true;
        } else {
            return false;
        }
    }

    public static String generateRegExp(String wildcard) {
        StringBuilder out = new StringBuilder("^");
        for (int i = 0; i < wildcard.length(); ++i) {
            final char c = wildcard.charAt(i);
            switch (c) {
                case '*':
                    out.append(".*");
                    break;
                case '?':
                    out.append('.');
                    break;
                case '.':
                    out.append("\\.");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                default:
                    out.append(c);
            }
        }
        out.append('$');
        return out.toString();
    }
}
