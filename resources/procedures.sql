CREATE PROCEDURE AverageDataVolume 
	PARTITION ON TABLE Average_Data_Volume COLUMN IP_A
AS
	SELECT (
		SELECT Payload_Sum 
    		FROM Average_Data_Volume 
    		WHERE IP_A = ? AND IP_B = ?)
    	/
    	(SELECT Payload_Count
    		FROM Average_Data_Volume
    		WHERE IP_A = ? AND IP_B = ?);


CREATE PROCEDURE AllActiveConnections
	PARTITITON ON TABLE Connections COLUMN Timestamp_Range
AS
	SELECT SRC_IP, SRC_PORT, DST_IP, DST_PORT
		FROM Connections
		WHERE Timestamp_Range = ? AND
		Timestamp between ? AND ?;


CREATE PROCEDURE ConnectionsToIpPort
	PARTITION ON TABLE Port_Connections COLUMN Dst_Ip_Port
AS 
	SELECT Src_Ip
	FROM Port_Connections
	WHERE Dst_Ip_Port = ?;


CREATE PROCEDURE IncommingConnectionsOnWKP
	PARTITION ON TABLE Well_Known_Ports COLUMN Dst_Ip
AS
	SELECT Dst_Ip, Dst_Port
	FROM Well_Known_Ports;


CREATE PROCEDURE SynFinRatio
	PARTITION ON TABLE Connections COLUMN Timestamp_Range
AS 
	SELECT (
		SELECT Count(*)
		FROM Connections
		WHERE Timestamp_Range = ? AND
		Timestamp between ? AND ?
		AND Flag = s)
		/
		)SELECT Count(*)
		FROM Connections
		WHERE Timestamp_Range = ? 
		AND Timestamp between ? AND ? 
		AND Flag = f);

CREATE PROCEDURE ByteSequence AS
	SELECT c.SRC_IP, c.SRC_Port, c.Dest_Ip, c.Dest_Port
		FROM Connections c
		JOIN Payload p ON Timestamp_Range AND c.Id = p.Connection_id
