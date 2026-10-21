package climaflu.mr;

/**
 * Parses one GHCN-Daily yearly-CSV line: ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME.
 * Applies the project's filter: US station, element in {TMAX, TMIN, PRCP}, empty quality flag.
 * The same rule is implemented in bigdata_pipeline/spark_tuning/BenchmarkWorkload.scala, bigdata_pipeline/spark_checks/DataFrameConsistency.scala and bigdata_pipeline/spark_tuning/benchmark_pandas.py.
 */
public final class GhcnRecord {
    public enum Outcome { KEPT, HEADER, MALFORMED, NOT_US, OTHER_ELEMENT, QC_FLAGGED }

    public String id;
    public String year;
    public String element;
    public long value;

    /** Parses {@code line} into this object and returns why it was kept or dropped. */
    public Outcome parse(String line) {
        String[] f = line.split(",", -1);
        if (f.length < 6) return Outcome.MALFORMED;
        if (f[0].equals("ID")) return Outcome.HEADER;
        if (!f[0].startsWith("US")) return Outcome.NOT_US;
        String el = f[2];
        if (!(el.equals("TMAX") || el.equals("TMIN") || el.equals("PRCP"))) return Outcome.OTHER_ELEMENT;
        if (!f[5].isEmpty()) return Outcome.QC_FLAGGED;
        if (f[1].length() < 4) return Outcome.MALFORMED;
        try {
            value = Long.parseLong(f[3]);
        } catch (NumberFormatException e) {
            return Outcome.MALFORMED;
        }
        id = f[0];
        year = f[1].substring(0, 4);
        element = el;
        return Outcome.KEPT;
    }
}
