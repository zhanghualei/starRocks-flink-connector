package com.nio.starRocks.flink.exception;

/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 14:10
 **/
public class StarRocksSystemException extends RuntimeException{

    public StarRocksSystemException(){
        super();
    }

    public StarRocksSystemException(String message){
        super(message);
    }

    public StarRocksSystemException(String message,Throwable cause){
        super(message,cause);
    }
    public StarRocksSystemException(Throwable cause){
        super(cause);
    }

    public StarRocksSystemException(String message,Throwable cause,boolean enableSuppression,
                                    boolean writableStackTrace){
        super(message,cause,enableSuppression,writableStackTrace);
  }
}
