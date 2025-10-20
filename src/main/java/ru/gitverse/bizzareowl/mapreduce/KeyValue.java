package ru.gitverse.bizzareowl.mapreduce;

import java.util.Objects;

public record KeyValue(String key, String value) {

    @Override
    public String toString() {
        return key + "=" + value;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass()) return false;
        KeyValue keyValue = (KeyValue) object;
        return Objects.equals(key, keyValue.key) && Objects.equals(value, keyValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
