DROP TABLE Average_Data_Volume;
DROP TABLE Connections;
DROP TABLE Payload;
DROP TABLE Port_Connections;
DROP TABLE Well_Known_Ports;


CREATE TABLE Connections (
	Timestamp_Range INTEGER NOT NULL,
	ID VARCHAR(50) NOT NULL,
	Timestamp TIMESTAMP NOT NULL,
	Src_IP VARCHAR(15) NOT NULL,
	Dst_IP VARCHAR(15) NOT NULL,
	Src_Port INTEGER NOT NULL,
	Dst_Port INTEGER NOT NULL,
	Flag VARCHAR(1),
	CONSTRAINT AC_PK PRIMARY KEY(Timestamp_Range, ID)
);
PARTITION TABLE Connections ON COLUMN Timestamp_Range;

CREATE TABLE Average_Data_Volume (
	IP_A VARCHAR(15) NOT NULL,
	IP_B VARCHAR(15) NOT NULL,
	Payload_Sum INTEGER DEFAULT '0' NOT NULL,
	Payload_Count INTEGER DEFAULT '0' NOT NULL,
	CONSTRAINT ADV_PK PRIMARY KEY(IP_A, IP_B)
);
PARTITION TABLE Average_Data_Volume ON COLUMN IP_A;

CREATE TABLE Port_Connections (
	Dst_IP_Port VARCHAR(20) NOT NULL,
	Src_IP VARCHAR(15) NOT NULL,
	CONSTRAINT IPC_PK PRIMARY KEY(Dst_IP_Port, Src_IP)
);
PARTITION TABLE Port_Connections ON COLUMN Dst_IP_Port;

CREATE TABLE Payload(
	Timestamp_Range INTEGER NOT NULL,
	Connection_ID VARCHAR(50) NOT NULL,
	Payload VARCHAR(10240) 
);

CREATE TABLE Well_Known_Ports(
	Dst_IP VARCHAR(15) NOT NULL,
	Dst_Port INTEGER NOT NULL
);
PARTITION TABLE Well_Known_Ports ON COLUMN Dst_IP;
