package com.hncy58.spark2.dbscan.invoker;

public interface Invoker<T, R> {
	
	R invoke(T data) throws Exception;
}
