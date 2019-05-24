package com.hncy58.spark2.submit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputStreamReaderRunnable implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(InputStreamReaderRunnable.class);

	private BufferedReader reader;
	private String name;

	public InputStreamReaderRunnable(InputStream is, String name) {
		this.reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
		this.name = name;
	}

	public void run() {
		System.out.println("InputStream " + name + ":");
		try {
			String line = reader.readLine();
			while (line != null) {
				// System.out.println(line);
				log.info(line);
				line = reader.readLine();
			}
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
	}
}