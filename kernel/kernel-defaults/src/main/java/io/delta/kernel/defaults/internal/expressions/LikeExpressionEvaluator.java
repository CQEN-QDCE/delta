/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.defaults.internal.expressions;

import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.internal.util.Utils;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.invalidEscapeSequence;
import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;

/**
 *
 */
public class LikeExpressionEvaluator {
    private LikeExpressionEvaluator() {
    }

    static Predicate validateAndTransform(
            Predicate like,
            Expression left, DataType leftOutputType,
            Expression right, DataType rightOutputType,
            Expression escapeCharExpr, DataType escapeCharOutputType) {

        if (!(StringType.STRING.equivalent(leftOutputType)
                && StringType.STRING.equivalent(rightOutputType))) {
            throw unsupportedExpressionException(like,
                    "'like' is only supported for string type expressions");
        }

        if (escapeCharExpr != null &&
                (!(escapeCharExpr instanceof Literal &&
                StringType.STRING.equivalent(escapeCharOutputType)))) {
            throw unsupportedExpressionException(like,
                    "'like' expects escape token expression to be a literal of String type");
        }

        Literal literal = (Literal) escapeCharExpr;
        if (literal!=null &&
                literal.getValue().toString().length() != 1) {
            throw unsupportedExpressionException(like,
                    "'like' expects escape token to be a single character");
        }

        return new Predicate(like.getName(), Arrays.asList(left, right, escapeCharExpr));
    }

    static ColumnVector eval(ColumnVector left,
                             ColumnVector right,
                             ColumnVector escapeCharLiteral) {
        final char DEFAULT_ESCAPE_CHAR = '\\';

        return new ColumnVector() {

            final char escape = escapeCharLiteral!=null ?
                    escapeCharLiteral.getString(0).charAt(0) : DEFAULT_ESCAPE_CHAR;


            @Override
            public DataType getDataType() {
                return BooleanType.BOOLEAN;
            }

            @Override
            public int getSize() {
                return left.getSize();
            }

            @Override
            public void close() {
                Utils.closeCloseables(left, right);
            }

            @Override
            public boolean getBoolean(int rowId) {
                return isLike(left.getString(rowId),
                        right.getString(rowId), escape);
            }

            @Override
            public boolean isNullAt(int rowId) {
                return left.isNullAt(rowId) || right.isNullAt(rowId);
            }

            public boolean isLike(String input, String pattern, char escape) {
                if (!Objects.isNull(input) && !Objects.isNull(pattern)) {
                    String regex =
                            escapeLikeRegex(pattern, escape);
                    return input.matches(regex);
                }
                return false;
            }

            /**
             * utility method to convert a predicate pattern to a java regex
             * @param pattern the pattern used in the expression
             * @param escape escape character to use
             * @return java regex
             */
            private String escapeLikeRegex(String pattern, char escape) {
                final int len = pattern.length();
                final StringBuilder javaPattern = new StringBuilder(len + len);
                for (int i = 0; i < len; i++) {
                    char c = pattern.charAt(i);

                    if (c == escape) {
                        if (i == (pattern.length() - 1)) {
                            throw invalidEscapeSequence(pattern, i);
                        }
                        char nextChar = pattern.charAt(i + 1);
                        if ((nextChar == '_')
                                || (nextChar == '%')
                                || (nextChar == escape)) {
                            javaPattern.append(
                                    Pattern.quote(Character.toString(nextChar)));
                            i++;
                        } else {
                            throw invalidEscapeSequence(pattern, i);
                        }
                    } else if (c == '_') {
                        javaPattern.append('.');
                    } else if (c == '%') {
                        javaPattern.append(".*");
                    } else {
                        javaPattern.append(Pattern.quote(Character.toString(c)));
                    }

                }
                return "(?s)" + javaPattern;
            }
        };
    }
}
