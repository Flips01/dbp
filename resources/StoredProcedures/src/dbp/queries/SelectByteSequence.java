package dbp.queries;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class SelectByteSequence extends VoltProcedure{
    public final SQLStmt select = new SQLStmt("SELECT * FROM PAYLOAD P WHERE P.PAYLOAD LIKE ?");

    public VoltTable[] run(String payload){
        voltQueueSQL(select, "%"+payload+"%");
        return voltExecuteSQL();
    }
}
