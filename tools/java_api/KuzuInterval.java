package tools.java_api;

public class KuzuInterval {
    public int months;
    public int days;
    public long micros;

    public KuzuInterval(int months, int days, long micros) {
        this.months = months;
        this.days = days;
        this.micros = micros;
    }
}
