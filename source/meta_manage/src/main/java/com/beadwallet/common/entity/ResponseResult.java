package com.beadwallet.common.entity;

import java.io.Serializable;

/**
 * @ClassName ResponseResult
 * @Description
 * @Author kai wu
 * @Date 2019/1/21 11:33
 * @Version 1.0
 */
public class ResponseResult<T> implements Serializable {

    /**
     * 编码
     */
    private Integer code;


    /**
     * 消息
     */
    private String msg;


    /**
     * 数据
     */
    private T data;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
