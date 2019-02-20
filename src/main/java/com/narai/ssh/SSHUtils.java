package com.narai.ssh;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Slf4j
public class SSHUtils {

    public static void initSSH() {
        try {
            String absKeyPath = Thread.currentThread().getContextClassLoader().getResource("id_rsa_work").toString();
            //getSession("172.81.238.160", "root", 22, 21330, "10.1.6.6", 3306, absKeyPath);
            getSessionPassword("跳板ip", "账号", 22, 28018, "跳转ip", 28018, "密码");
        } catch (Throwable e) {
            log.error("", e);
        }
        log.info("......結束初始化");
    }

    private static void getSession(String host, String user, Integer port, Integer oldPort, String newip, Integer newPort, String absKeyPath) {
        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(user, host, port);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.setTimeout(1000);
            session.setConfig(config);
            jsch.addIdentity(absKeyPath.replaceAll("file:", ""));
            session.connect();
            session.setPortForwardingL(oldPort, newip, newPort);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    private static void getSessionPassword(String host, String user, Integer port, Integer oldPort, String newip, Integer newPort, String password) {
        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(user, host, port);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.setTimeout(1000);
            session.setConfig(config);
            session.setPassword(password);
            session.connect();
            session.setPortForwardingL(oldPort, newip, newPort);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

}
