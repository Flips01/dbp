package de.hsh.inform.dbp_project_readdata;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.ArpPacket;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.FragmentedPacket;
import org.pcap4j.packet.IcmpV4CommonPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.namednumber.EtherType;
import org.pcap4j.packet.namednumber.IpNumber;

/**
 * A small sample application that reads a pcap file and interprets its contents.
 * Be aware that more cases might occur! Your database model should be able to
 * cope with other cases as well.
 *
 * The pcap file on the skripte server is compressed and must be uncompressed before
 * reading it with this application.
 */
public class App {

	public static VoltPacket current = null;
	public static ArrayList<VoltPacket> packets = new ArrayList<>();

	public static void main(String[] args) throws PcapNativeException, EOFException, TimeoutException, NotOpenException {

		if (args.length == 0) {
			System.out.println("usage: readtcpdata <filename>");
			System.exit(1);
		}
		
		String filename = args[0];
		PcapHandle handle = Pcaps.openOffline(filename);
		int cnt = 0;
		final int LIMIT = 100;
		for (;;) {
			// loop over all packets in the pcap file
			
			Packet packet = null;
			try {
				packet = handle.getNextPacketEx();
			} catch(PcapNativeException ex) {
				System.out.println(ex.toString());
				// file is truncated. ignore exception
			}
			if (packet == null /*|| packets.size() > LIMIT*/)
				break;
			current = new VoltPacket();
			// read basic packet data: timestamp and length
			long ts = handle.getTimestampInts();
			int ms = handle.getTimestampMicros();
			int len = packet.length();

			current.setTs(ts);
			current.setMs(ms);
			current.setLength(len);

			//System.out.printf("New Packet. TS=%d/%d, len=%d\n", ts, ms, len);
			
			// this caputure only contains ethernet packets
			// for other data, this must be modified.

			EthernetPacket ether = packet.get(EthernetPacket.class);
			handleEthernetPacket(ether, "  ");
			cnt++;
		}	
		handle.close();
		//System.out.println("Number of packets: " + cnt);
		try {
			/*ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File("./out/data.bin")));
			oos.writeObject(packets);
			oos.close();*/
			Gson out = new GsonBuilder().registerTypeHierarchyAdapter(byte[].class, new ByteDataSerializer()).create();
			FileWriter writer = new FileWriter(new File("out/"+args[0].split("/")[1]+".json"));
			out.toJson(packets, writer);
			writer.write("");
			writer.flush();
			writer.close();
			System.out.println("Done writing");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void handleEthernetPacket(EthernetPacket ether, String prefix) {
		// depending on the ethernet type, interpret the contents of the ethernet
		// frame in different ways
		EtherType etherType = ether.getHeader().getType();
		//System.out.println(prefix + "EtherType " + etherType);
		if (etherType.equals(EtherType.IPV4)) {
			IpV4Packet ipv4 = ether.getPayload().get(IpV4Packet.class);
			handleIpV4Packet(ipv4, prefix + "  ");
		} else if (ether.getHeader().getType().equals(EtherType.ARP)) {
			ArpPacket arp = ether.getPayload().get(ArpPacket.class);
			handleArpPacket(arp, prefix + "  ");
		} else {
			//System.out.println(prefix + "EtherType unknown, no further processing");
			// unknown type
		}
		
	}
	
	public static void handleIpV4Packet(IpV4Packet ipv4, String prefix) {

		IpNumber ipnum = ipv4.getHeader().getProtocol();
		//System.out.printf(prefix + "Ip Protocol Number: %s\n", ipnum.toString());
		current.setSrcIP(ipv4.getHeader().getSrcAddr().getHostAddress());
		current.setDstIP(ipv4.getHeader().getDstAddr().getHostAddress());
		if (ipv4.getPayload() instanceof FragmentedPacket) {
			//System.out.println(prefix + "Fragemented Packet");
		} else if (ipnum.equals(IpNumber.TCP)) {
			TcpPacket tcp = ipv4.getPayload().get(TcpPacket.class);
			handleTcpPacket(tcp, prefix + "  ");
		} else if (ipnum.equals(IpNumber.UDP)) {
			UdpPacket udp = ipv4.getPayload().get(UdpPacket.class);
			handleUdpPacket(udp, prefix + "  ");
		} else if (ipnum.equals(IpNumber.ICMPV4)) {
			IcmpV4CommonPacket icmp = ipv4.getPayload().get(IcmpV4CommonPacket.class);
			handleIcmpPacket(icmp, prefix + "  ");
		} else {
			//System.out.println(prefix + "Unknown protocol, no further processing");
		}
	}
	
	public static void handleArpPacket(ArpPacket arp, String prefix) {
		//System.out.println(prefix + "Storing ARP packet");
	}
	
	public static void handleTcpPacket(TcpPacket tcp, String prefix) {
		current.setSrcPort(tcp.getHeader().getSrcPort().valueAsInt());
		current.setDstPort(tcp.getHeader().getDstPort().valueAsInt());
		current.setFIN(tcp.getHeader().getFin());
		current.setSYN(tcp.getHeader().getSyn());
		current.setType('t');
		//System.out.println(prefix + "Storing TCP packet");
		finishPacket(tcp);
	}
	
	public static void handleUdpPacket(UdpPacket udp, String prefix)  {
		current.setSrcPort(udp.getHeader().getSrcPort().valueAsInt());
		current.setDstPort(udp.getHeader().getDstPort().valueAsInt());
		current.setType('u');
		//System.out.println(prefix + "Storing UDP packet");
		finishPacket(udp);
	}

	public static void finishPacket(Packet p){
		if(p.getPayload() != null)current.setPayload(p.getPayload().getRawData());
		packets.add(current);
	}

	public static void handleIcmpPacket(IcmpV4CommonPacket icmp, String prefix) {
		//System.out.println(prefix + "Storing ICMP packet");
	}


}
