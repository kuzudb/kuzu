package tools.java_api;

public class KuzuFlatTuple {
    long ft_ref;
    boolean destoryed = false;

    private void checkNotDestoryed () {
        assert !destoryed: "FlatTuple has been destoryed.";
    }

    public void destroy () {
        checkNotDestoryed();
        KuzuNative.kuzu_flat_tuple_destroy(this);
        destoryed = true;
    }

    public KuzuValue getValue (long index) {
        checkNotDestoryed();
        return KuzuNative.kuzu_flat_tuple_get_value(this, index);
    }

    public String toString () {
        checkNotDestoryed();
        return KuzuNative.kuzu_flat_tuple_to_string(this);
    }
}
