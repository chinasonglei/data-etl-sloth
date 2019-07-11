package com.beadwallet.common.utils.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * @ClassName CommonUtil
 * @Description
 * @Author kai wu
 * @Date 2019/3/25 11:25
 * @Version 1.0
 */
public class CommonUtil {

    /**
     * 用于删除字符串中指定的非法字符
     * @Param [s]
     * @return java.lang.String
     **/
    public static String convertIllegalCharacter(String s){
        //非法字符集{"\u007F":"DEL (delete)","\u0027":"'"}
        String[] illegalCharacters = {"\u007F","\u0027"};

        int size = illegalCharacters.length;
        while(size>0){
            s = s.replace(illegalCharacters[size-1],"") ;
            size--;
        }
       return s;
    }


    public static String ClobToString(Clob clob) throws SQLException, IOException {
        String reString = "";
        Reader is = clob.getCharacterStream();
        BufferedReader br = new BufferedReader(is);
        String s = br.readLine();
        StringBuffer sb = new StringBuffer();
        while (s != null) {
            sb.append(s);
            s = br.readLine();
        }
        reString = sb.toString();
        if(br!=null){
            br.close();
        }
        if(is!=null){
            is.close();
        }
        return reString;
    }


    public static Clob stringToClob(String str) {
        if (null == str){
            return null;
        } else {
            try {
                java.sql.Clob c = new javax.sql.rowset.serial.SerialClob(str
                        .toCharArray());
                return c;
            } catch (Exception e) {
                return null;
            }
        }
    }

}
