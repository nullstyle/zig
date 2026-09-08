// A translated shift amount gets wrapped in @intCast, which does not infer
// the result type of a sign-changing cast's @bitCast operand.
static inline unsigned long long shift_unsigned(unsigned long long value, int amount) {
    value <<= (unsigned long long)amount;
    value >>= (unsigned long long)(amount - 1);
    return value;
}

static inline unsigned long long shift_signed(unsigned long long value, unsigned int amount) {
    value <<= (long long)amount;
    return value;
}

static inline unsigned long long widen_signed(int value) {
    return (unsigned long long)value;
}

static inline int narrow_unsigned(unsigned long long value) {
    return (int)value;
}
