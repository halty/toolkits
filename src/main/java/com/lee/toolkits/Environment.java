package com.lee.toolkits;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/** an application environment provided ip, app name, deploy env, machine id etc. **/
public final class Environment {

	/** application environment collation **/
	public static enum Env {
		UNKNOWN(0, "unknown"),
		DEV(1, "development"),
		ALPHA(2, "alpha"),
		BETA(3, "beta"),
		PRE(4, "stage"),
		ONLINE(5, "product"),
		PERF(6, "performance")
		;
		
		public final int type;
		public final String name;
		private Env(int type, String name) {
			this.type = type;
			this.name = name;
		}
	}

	/** ipv4 address **/
	private final String ip;
	/** application name **/
	private final String appName;
	/** deploy environment **/
	private final Env env;
	/**
	 * deploy machine serial number per {@code appName} per {@code env}.
	 * <p>
	 * different {@code appName} deploy in the same {@code env}, might
	 * have the same {@code machineId};
	 * the same {@code appName} deploy in different {@code env}, might
	 * have the same {@code machineId};
	 * </p>
	 */
	private final int machineId;
	
	private Environment(String ip, String appName, String envName, int machineId) {
		this.ip = fetchLocalIp(ip);
		this.appName = appName;
		this.env = getEnv(envName);
		this.machineId = machineId;
	}
	
	public String ip() { return ip; }
	
	public String appName() { return appName; }
	
	public Env currentEnv() { return env; }
	
	public int machineId() { return machineId; }
	
	/** load current application environment **/
	public static Environment load() { return Holder.ENVIRONMENT; }
	
	private static class Holder {
		private static final Environment ENVIRONMENT = load();
		
		private static Environment load() {
			Environment environment = new Environment("192.168.1.1", "toolkits", "beta", 1);
			/* might load from
			 * 1) local properties file
			 * 2) system properties
			 * 3) system environment variables
			 * 4) network configuration stream
			 * 5) unified configuration service, e.g. zookeeper
			 */
			return environment;
		}
	}
	
	private static String fetchLocalIp(String defaultIp) {
        String serverIp = "";
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface interfaceN = (NetworkInterface) interfaces.nextElement();
                Enumeration<InetAddress> ienum = interfaceN.getInetAddresses();
                while (ienum.hasMoreElements()) {
                    InetAddress ia = ienum.nextElement();
                    if (ia instanceof Inet4Address) {
                        if (ia.getHostAddress().toString().startsWith("127")) {
                            continue;
                        } else {
                            serverIp = ia.getHostAddress();
                            break;
                        }
                    }
                }
                if(!serverIp.isEmpty()) {
                	serverIp = "";	// multiple nic, multiple ip, so use default ip
                	break;
                }
            }
        }catch(Exception e) { /** ignore **/ }
        return serverIp.isEmpty() ? defaultIp : serverIp;
    }
	
	private static Env getEnv(String envName) {
		for(Env env : Env.values()) {
			if(env.name.equals(envName)) { return env; }
		}
		return Env.UNKNOWN;
	}
}
