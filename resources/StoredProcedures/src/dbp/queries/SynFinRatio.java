package dbp.queries;

import org.voltdb.*;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/*
RRetrieve the ratio of SYN packets to FIN packets in a given time period
(only TCP packets)

 */
public class SynFinRatio extends VoltProcedure{

	private final int partitionInterval = 1800;

    public final SQLStmt getCount = new SQLStmt(
            "SELECT COUNT(Flag) Occurrence, Flag "
				+"FROM Connections "
				+"WHERE Timestamp_Range = ? AND "
				+"Timestamp BETWEEN ? AND ? "
				+"Group By Flag;");

    public VoltTable run(Timestamp startTime, Timestamp endTime) throws VoltAbortException{

		long startHour = partitionTs((int)startTime.getTime()/1000, partitionInterval);
        long endHour = partitionTs((int)endTime.getTime()/1000, partitionInterval);
		
		long queryHours = endHour-startHour;
		
		long synCount = 0;
		long finCount = 0;

		for (int i = 0; i < queryHours; ++i){
			voltQueueSQL(getCount, startHour+i, startTime, endTime);
        	VoltTable[] result = voltExecuteSQL();

			for(int j = 0; j < result[0].getRowCount(); j++) {
				if (result[0].fetchRow(j).getString("Flag").equalsIgnoreCase("s")){
					synCount += result[0].fetchRow(j).getLong("Occurrence");
				}
				if(result[0].fetchRow(j).getString("Flag").equalsIgnoreCase("f")){
					finCount += result[0].fetchRow(j).getLong("Occurrence");
				}
			}
        }

        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("SynFinRatio", VoltType.FLOAT), new VoltTable.ColumnInfo("startHour",VoltType.INTEGER), new VoltTable.ColumnInfo("endHour",VoltType.INTEGER));

        if(finCount > 0){
			result.addRow(synCount/(float)finCount, startHour, endHour);
		}else{
			result.addRow((float)synCount, startHour, endHour);
		}

		return result;
    }

	private long convertTimestamp(Timestamp ts){
		LocalDateTime timeLocal = ts.toLocalDateTime();
        timeLocal = timeLocal.minusMinutes(timeLocal.getMinute()).minusSeconds(timeLocal.getSecond()).minusNanos(timeLocal.getNano());
        //Dangerous time magic
        Timestamp hour = Timestamp.from(timeLocal.toInstant(ZoneOffset.ofHours(0)));

		return hour.getTime()/(1000*60*60);
	}

	private int partitionTs(int ts, int partitionInterval) {
		int p = ts / partitionInterval;
		return p*partitionInterval;
	}
}
