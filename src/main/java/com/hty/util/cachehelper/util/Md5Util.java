package com.hty.util.cachehelper.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Util {
	
	public static String getMd5(String input){
		return getMd5(input.getBytes());
	}
	public static String getMd5(byte[] input) {
		MessageDigest md5digest = null;
		try {
			md5digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) { }
		md5digest.update(input);
		BigInteger big = new BigInteger(1, md5digest.digest());
		String md5 = big.toString(16);
		return md5;
	}
}
