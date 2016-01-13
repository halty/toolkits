package com.lee.toolkits;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * generate a unique sequence within a web cluster which use to
 * mark a unique request.
 */ 
public final class BizReqSequenceGenerator {
	
	private static final Environment ENV = Environment.load();

	private BizReqSequenceGenerator() {}
	
	/** return unique request sequence based on {@link UUID} without separator {@code '-'} **/
	public static String uuidReqSeq() { return UUID.randomUUID().toString().replace("-", ""); }
	
	/** return date time string with format: {@code yyyyMMddHHmmss} **/
    public static String dateTimeFormat() { return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()); }
    
    /** return date string with format: {@code yyyyMMdd} **/
    public static String dateFormat() { return new SimpleDateFormat("yyyyMMdd").format(new Date()); }
    
    /** return time string with format: {@code HHmmss} **/
    public static String timeFormat() { return new SimpleDateFormat("HHmmss").format(new Date()); }
    
    private static final long IP = ipStrToLong(ENV.ip());
    private static final AtomicInteger SEQ_1 = new AtomicInteger(0);
    private static final int MAX_SEQ_1 = 10000;
    
    /**
     * return unique sequence with format: {@code seq(5) + env(1) + ip(10)}.
     * guarantee that max {@code 100,000} concurrent requests within a web cluster
     */
    public static String sequenceBasedOnIp() {
    	int seq = SEQ_1.getAndIncrement() % MAX_SEQ_1;
    	if(seq < 0) { seq += MAX_SEQ_1; }
    	int env = ENV.currentEnv().type;
    	return String.format("%05d%d%010d", seq, env, IP);
    }
    
    /**
     * return unique request sequence based on ip and date time with format: 
     * {@code date(8) + time(6) + seq(5) + env(1) + ip(10)}.
     * <p>it supports up to {@code 100,000} concurrent requests within a web cluster.</p>
     */
    public static String ipBasedReqSeq() { return dateTimeFormat() + sequenceBasedOnIp(); }
    
    private static final int MACHINE_ID = ENV.machineId() % 10000;
    private static final AtomicInteger SEQ_2 = new AtomicInteger(0);
    private static final int MAX_SEQ_2 = 10000;
    
    /**
     * return unique sequence with format: {@code seq(5) + env(1) + machineId(4)}.
     * guarantee that max {@code 100,000} concurrent requests within a web cluster
     * which support up to {@code 10,000} machines.
     */
    public static String sequenceBasedOnMachineId() {
    	int seq = SEQ_2.getAndIncrement() % MAX_SEQ_2;
    	if(seq < 0) { seq += MAX_SEQ_2; }
    	int env = ENV.currentEnv().type;
    	return String.format("%05d%d%04d", seq, env, MACHINE_ID);
    }
    
    /**
     * return unique request sequence based on machine id and date time with format: 
     * {@code date(8) + time(6) + seq(5) + env(1) + machineId(4)}.
     * <p> it supports up to{@code 100,000} concurrent requests within a web cluster
     * which support up to {@code 10,000} machines.</p>
     */
    public static String machineIdBasedReqSeq() { return dateTimeFormat() + sequenceBasedOnMachineId(); }
    
    /** convert ipv4 address format from string to dotted decimal integer **/
    static long ipStrToLong(String ip) {
        ip = ip.trim();
        if(ip.isEmpty()) { return 0; }
        String[] fields = ip.split("\\.", 4);
        int i = 0;
        long result = 0;
        for(String field : fields) {
            result <<= 8;
            int b = Integer.valueOf(field) & 0xff;
            result |= b;
            i++;
        }
        while(i < 4) {
            result <<= 8;
            i++;
        }
        
        return result;
    }
    
    /** convert ipv4 address format from dotted decimal integer to string **/
    static String ipLongToStr(long ip) {
        StringBuilder buf = new StringBuilder(16);
        for(int i=3, shift=i*8; i>=0; i--, shift-=8) {
            long field = (ip >> shift) & 0xff;
            buf.append(field).append('.');
        }
        buf.setLength(buf.length()-1);
        return buf.toString();
    }
}
