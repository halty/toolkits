package com.lee.toolkits;

public class BizReqSequenceGeneratorTest {

	public static void main(String[] args) {
		String bizSeq = BizReqSequenceGenerator.ipBasedReqSeq();
		System.out.println(bizSeq);
		String ip = bizSeq.substring(bizSeq.length()-10, bizSeq.length());
		System.out.println(BizReqSequenceGenerator.ipLongToStr(Long.valueOf(ip)));
		
		bizSeq = BizReqSequenceGenerator.machineIdBasedReqSeq();
		System.out.println(bizSeq);
	}

}
