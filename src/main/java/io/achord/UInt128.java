package io.achord;

/**
 * @author Camelion
 * @since 15/02/2018
 */
final class UInt128 {
    final long first;
    final long second;

    UInt128(long first, long second) {
        this.first = first;
        this.second = second;
    }

    static UInt128 of(long first, long second) {
        return new UInt128(first, second);
    }
}
