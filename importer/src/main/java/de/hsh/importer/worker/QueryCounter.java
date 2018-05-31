package de.hsh.importer.worker;

public class QueryCounter {
    private long queriesIssued;
    private long queriesCompleted;

    public QueryCounter() {
        this.queriesIssued = 0;
        this.queriesCompleted = 0;
    }

    public synchronized void queryIssued() {
        this.queriesIssued++;
    }

    public synchronized void queryCompleted() {
        this.queriesCompleted++;
    }

    public long getQueriesCompleted() {
        return this.queriesCompleted;
    }

    public long getQueriesIssued() {
        return this.queriesIssued;
    }

    public double getCompletionRate() {
        double completed = this.queriesCompleted;
        double issued = this.queriesIssued;
        return  completed/issued;
    }
}
