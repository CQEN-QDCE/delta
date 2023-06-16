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

package io.delta.kernel.types;

import java.util.Objects;

/**
 * The data type representing {@code java.math.BigDecimal} values.
 * A Decimal that must have fixed precision (the maximum number of digits) and scale (the number
 * of digits on right side of dot).
 * <p>
 * The precision can be up to 38, scale can also be up to 38 (less or equal to precision).
 * <p>
 * The default precision and scale is (10, 0).
 */
public final class DecimalType extends DataType
{
    public static final DecimalType USER_DEFAULT = new DecimalType(10, 0);

    private final int precision;
    private final int scale;

    public DecimalType(int precision, int scale)
    {
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * @return the maximum number of digits of the decimal
     */
    public int getPrecision()
    {
        return precision;
    }

    /**
     * @return the number of digits on the right side of the decimal point (dot)
     */
    public int getScale()
    {
        return scale;
    }

    @Override
    public String toJson()
    {
        return String.format("\"decimal(%d, %d)\"", precision, scale);
    }

    @Override
    public String toString()
    {
        return String.format("Decimal(%d, %d)", precision, scale);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DecimalType that = (DecimalType) o;
        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, scale);
    }
}
