package com.demo.余弦相似度.util;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicFloat extends Number{

    private AtomicInteger bits;


    public AtomicFloat() {
        this(0f);
    }

    public AtomicFloat(float initialValue) {
        bits = new AtomicInteger(Float.floatToIntBits(initialValue));
    }

    /**
     * 叠加
     *
     * @param delta
     * @return
     */
    public final float addAndGet(float delta) {
        float expect;
        float update;
        do {
            expect = get();
            update = expect + delta;
        } while (!this.compareAndSet(expect, update));

        return update;
    }

    public final float getAndAdd(float delta) {
        float expect;
        float update;
        do {
            expect = get();
            update = expect + delta;
        } while (!this.compareAndSet(expect, update));

        return expect;
    }

    public final float getAndDecrement() {
        return getAndAdd(-1);
    }

    public final float decrementAndGet() {
        return addAndGet(-1);
    }

    public final float getAndIncrement() {
        return getAndAdd(1);
    }

    public final float incrementAndGet() {
        return addAndGet(1);
    }

    public final float getAndSet(float newValue) {
        float expect;
        do {
            expect = get();
        } while (!this.compareAndSet(expect, newValue));

        return expect;
    }

    public final boolean compareAndSet(float expect, float update) {
        return bits.compareAndSet(Float.floatToIntBits(expect), Float.floatToIntBits(update));
    }

    public final void set(float newValue) {
        bits.set(Float.floatToIntBits(newValue));
    }

    public final float get() {
        return Float.intBitsToFloat(bits.get());
    }


    /**
     * Returns the value of the specified number as an {@code int},
     * which may involve rounding or truncation.
     *
     * @return  the numeric value represented by this object after conversion
     *          to type {@code int}.
     */
    @Override
    public int intValue() {
        return (int) get();
    }


    /**
     * Returns the value of the specified number as a {@code long},
     * which may involve rounding or truncation.
     *
     * @return  the numeric value represented by this object after conversion
     *          to type {@code long}.
     */
    @Override
    public long longValue() {
        return (long)get();
    }


    /**
     * Returns the value of the specified number as a {@code float},
     * which may involve rounding.
     *
     * @return  the numeric value represented by this object after conversion
     *          to type {@code float}.
     */
    @Override
    public float floatValue() {
        return get();
    }


    /**
     * Returns the value of the specified number as a {@code double},
     * which may involve rounding.
     *
     * @return  the numeric value represented by this object after conversion
     *          to type {@code double}.
     */
    @Override
    public double doubleValue() {
        return (double) get();
    }

    public static void main(String[] args) {
        Float x1 = 1.2F;
        Float x2 = 1.2F;
        float oneOfTheDimension = x1 * x1;
        // +
        AtomicFloat aa = new AtomicFloat(2);
        aa.addAndGet(oneOfTheDimension);

        System.out.printf(aa.get()+"");
    }
}
