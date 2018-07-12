package com.unistack.tamboo.message.kafka.runtime;

import com.google.common.base.MoreObjects;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 */
public abstract class AbstractStatus<T> {

    public enum State {
        RUNNING,
        SHUTDOWN,
        FAILED,
        DESTROYED;
    }

    private final T id;
    private final State state;
    private final String trace;

    protected AbstractStatus(T id, State state, String trace) {
        this.id = id;
        this.state = state;
        this.trace = trace;
    }


    public T runnerId() {
        return id;
    }

    public State state() {
        return state;
    }

    public String trace() {
        return trace;
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("state", state)
                .add("trace", trace)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractStatus<?> that = (AbstractStatus<?>) o;

        if (!id.equals(that.id)) return false;
        if (state != that.state) return false;
        return trace.equals(that.trace);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + state.hashCode();
        result = 31 * result + trace.hashCode();
        return result;
    }
}
