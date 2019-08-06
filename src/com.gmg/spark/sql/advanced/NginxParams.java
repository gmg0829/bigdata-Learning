package com.gmg.spark.sql.advanced;

import java.io.Serializable;

/**
 * @author gmg
 * @title: NginxParams
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/6 16:20
 */
public class NginxParams implements Serializable {
    private String remoteAddr;

    private String remoteUser;

    private String timeLocal;

    private String request;

    private String status;

    private String byteSent;

    private String referer;

    private String httpUserAgent;

    private String httpForwardedFor;

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getByteSent() {
        return byteSent;
    }

    public void setByteSent(String byteSent) {
        this.byteSent = byteSent;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
    }

    public String getHttpForwardedFor() {
        return httpForwardedFor;
    }

    public void setHttpForwardedFor(String httpForwardedFor) {
        this.httpForwardedFor = httpForwardedFor;
    }

    @Override
    public String toString() {
        return "NginxParams{" +
                "remoteAddr='" + remoteAddr + '\'' +
                ", remoteUser='" + remoteUser + '\'' +
                ", timeLocal='" + timeLocal + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                ", byteSent='" + byteSent + '\'' +
                ", referer='" + referer + '\'' +
                ", httpUserAgent='" + httpUserAgent + '\'' +
                ", httpForwardedFor='" + httpForwardedFor + '\'' +
                '}';
    }
}