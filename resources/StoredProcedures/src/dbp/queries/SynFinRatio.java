package dbp.queries;

import org.voltdb.*;

import java.sql.Timestamp;


/*
Retrieve the ratio of SYN packets to FIN packets in a given time period
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

		int[] partitions = partitionsInRange((int)startTime.getTime()/1000, (int)endTime.getTime()/1000, partitionInterval);

		long synCount = 0;
		long finCount = 0;

		for (int i = 0; i < partitions.length; ++i){
			voltQueueSQL(getCount, partitions[i], startTime, endTime);
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

        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("SynFinRatio", VoltType.FLOAT));

        if(finCount > 0){
			result.addRow(synCount/(float)finCount);
		}else{
			result.addRow((float)synCount);
		}

		return result;
    }

    //Create array of Timestamp_Ranges the query data are in

    private int[] partitionsInRange(int start, int end, int partitionInterval) {
        int start_part = partitionTs(start, partitionInterval);
        int end_part = partitionTs(end, partitionInterval);

        int part_range = end_part-start_part;
        int partitions = (part_range/partitionInterval)+1;

        int[] parts = new int[partitions];
        for(int i=0; i<partitions; i++) {
            parts[i] = start_part + (i*partitionInterval);
        }
        return parts;
    }

    //Converts Timestamp to Timestamp_Range

	private int partitionTs(int ts, int partitionInterval) {
		int p = ts / partitionInterval;
		return p*partitionInterval;
	}
}
