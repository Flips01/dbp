import org.voltdb.*;

import java.sql.Timestamp;

/*
RRetrieve the ratio of SYN packets to FIN packets in a given time period
(only TCP packets)

 */
public class SynFinRatio extends VoltProcedure{

    public final SQLStmt getSynCount = new SQLStmt(
            "SELECT Count(*)"
            + "FROM Connections "
            + "WHERE Flag = 's' "
            + "AND Timestamp_Range = ? "
            + "AND Timestamp between ? AND ?");

    public final SQLStmt getFinCount = new SQLStmt(
            "SELECT Count(*)"
            + "FROM Connections "
            + "WHERE Flag = 'f' "
            + "AND Timestamp_Range = ? "
            + "AND Timestamp between ? AND ?");

    public long run(Timestamp startTime, Timestamp endTime) throws VoltAbortException{
        int startHour = startTime.toLocalDateTime().getHour();
        int endHour = endTime.toLocalDateTime().getHour();

        voltQueueSQL(getSynCount, startHour, startTime, endTime);
        VoltTable[] synResult = voltExecuteSQL();
        long synCount = synResult[0].fetchRow(0).getLong(0);

        voltQueueSQL(getFinCount, startHour, startTime, endTime);
        VoltTable[] finResult = voltExecuteSQL();
        long finCount = finResult[0].fetchRow(0).getLong(0);

        if(startHour != endHour){
            voltQueueSQL(getSynCount, endHour, startTime, endTime);
            VoltTable[] result2 = voltExecuteSQL();
            synCount += result2[0].fetchRow(0).getLong(0);

            voltQueueSQL(getFinCount, endHour, startTime, endTime);
            VoltTable[] finResult2 = voltExecuteSQL();
            finCount += finResult2[0].fetchRow(0).getLong(0);
        }

        return synCount/finCount;
    }
}
