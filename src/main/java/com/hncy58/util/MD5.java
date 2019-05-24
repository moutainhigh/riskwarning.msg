package com.hncy58.util;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MD5 {

	private static final String DEFAULT_ENCODE = Charset.defaultCharset().name();
	private static final char MD5_STR[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	public final static String MD5(String source, String encoding, boolean upercase) throws Exception {
		
		encoding = encoding == null || !Charset.isSupported(encoding) ? DEFAULT_ENCODE : encoding;

		// 使用平台的默认字符集将此 String 编码为 byte序列，并将结果存储到一个新的 byte数组中
		byte[] btInput = source.getBytes(encoding);
		
		// 信息摘要是安全的单向哈希函数，它接收任意大小的数据，并输出固定长度的哈希值。
		MessageDigest mdInst = MessageDigest.getInstance("MD5");
		
		// MessageDigest对象通过使用 update方法处理数据， 使用指定的byte数组更新摘要
		mdInst.update(btInput);
		
		// 摘要更新之后，通过调用digest（）执行哈希计算，获得密文
		byte[] md = mdInst.digest();
		
		// 把密文转换成十六进制的字符串形式
		int j = md.length;
		char str[] = new char[j * 2];
		int k = 0;
		for (int i = 0; i < j; i++) { // i = 0
			byte byte0 = md[i]; // 95
			str[k++] = MD5_STR[byte0 >>> 4 & 0xf]; // 5
			str[k++] = MD5_STR[byte0 & 0xf]; // F
		}
		
		// 返回经过加密后的字符串
		return upercase ? new String(str).toUpperCase() : new String(str);
	}
	
	private static final String suffixKey = "32sdfjoi3onmsdff";
	
	public static String encode(String toEncodeString) {
		String result = "";
		try {
			result = MD5(toEncodeString + suffixKey, null, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		System.out.println(result);
		return result;
	}
	
	public static boolean isValid(String str) {
		if(null == str || str.length() < 32) {
			return false;
		}
		return str.equals(encode(str.substring(32)));
	}

	public static void main(String[] args) throws Exception {

		String username = "hncy58";
		long date = System.currentTimeMillis();
		System.out.println("oldDate:" + date);
		int size = 10000;
		
		String encodeStr = encode(username + date);
		String retStr = encodeStr + date;
		
		System.out.println(encodeStr);
		System.out.println(retStr);
		
		long retDate = Long.valueOf(retStr.substring(32));
		
		System.out.println("retDate:" + retDate);
		
		Thread.sleep(1000);
		
		date = System.currentTimeMillis();
		System.out.println("newDate:" + date);
		long diff = date - retDate;
		
		if(diff < -3600 || diff > 3600) {
			System.out.println("过期Token");
		} else {
			System.out.println("未过期Token");
		}
		
		

//		System.out.println(MD5(prefix, null, false));
//
//		long start = System.currentTimeMillis();
//		for (int i = 0; i < size; i++) {
//			MD5(prefix + i, "UTF-8", false);
//		}
//		System.out.println("MD5 used:" + (System.currentTimeMillis() - start) + "ms.");
		
		
		System.out.println(MD5("12345678啊" + suffixKey, null, false));
		System.out.println(MD5("12345678啊" + suffixKey, "utf-8", false));
		System.out.println(MD5("12345678啊" + suffixKey, "gbk", false));
	}
}
