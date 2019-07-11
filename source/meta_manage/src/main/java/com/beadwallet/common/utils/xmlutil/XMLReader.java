package com.beadwallet.common.utils.xmlutil;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName XMLReader
 * @Description
 * @Author kai wu
 * @Date 2019/1/9 9:22
 * @Version 1.0
 */
public class XMLReader {

    static Logger logger = LoggerFactory.getLogger(XMLReader.class);

    /**
     * editor: lzy
     * last modify: 2019-01-09
     *
     * @param tag <mysqljdbc & hivejdbc>
     * @return a list of jdbcConnectionArray for JDBCConnectionFactory to Create Connections
     */
    public static ArrayList getXMInfo(String className, String filePath, String tag) {
        ArrayList xmlInfoList = new ArrayList();
        //XMLBuilder
        SAXBuilder xmlBuilder = new SAXBuilder();
        Document doc = null;
        File file = null;
        Object instance = null;
        try {
            //读取xml文件
            file = new File(filePath);
            //文件不存在
            if (!file.exists()) {
                System.out.println("FILE IS NOT AVILIBLE");
            }
            doc = xmlBuilder.build(file);
            //new BufferedReader(new InputStreamReader(new FileInputStream(filepath),"UTF-8"));
            //xmlBuilder.build(new InputSource(new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF-8"))));
            //拿到根元素
            Element rootElement = doc.getRootElement();
            //拿到我们需要的数据库连接类型类型
            List elementList = rootElement.getChildren(tag);
            //遍历拿到类型匹配的子节点
            for (int i = 0; i < elementList.size(); i++) {
                //类型转换
                Element element = (Element) elementList.get(i);
                //通过反射创建对象实例
                Class classname = Class.forName(className);
                Constructor constructor = classname.getConstructor();
                instance = constructor.newInstance();
                //获取所有的成员变量
                Field[] fields = classname.getDeclaredFields();
                //加强版list复杂数据类型的封装
                HashMap<Object, ArrayList> firldMap = new HashMap();

                instance = encapsulationByReflex(instance, fields, firldMap, element);
                xmlInfoList.add(instance);
            }
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return xmlInfoList;
        }
    }

    /***
     * editor: lzy
     * last modify: 2019-01-17
     *
     * @param instance reflex instance
     * @param fields fields of instance
     * @param fieldMap
     * @param element
     * @return
     * @throws IllegalAccessException
     */
    private static Object encapsulationByReflex(Object instance, Field[] fields, HashMap<Object, ArrayList> fieldMap, Element element) throws IllegalAccessException {
        //循环获取成员变量的名称 split[length-1]
        for (int j = 0; j < fields.length; j++) {
            //私有成员变量开放操作权限
            fields[j].setAccessible(true);
            //拿到具体的对象名
            String[] split = fields[j].toString().split("\\.");
            //拿到对象名对应的xml信息,封装到map<key,Array>里
            List<Element> children = element.getChildren(split[split.length - 1]);
            if (children.size() >= 1) {
                for (Element child : children) {
                    ArrayList container = null;
                    //System.out.println(child.getValue());
                    if (!fieldMap.containsKey(split[split.length - 1])) {
                        //key不存在的情况
                        container = new ArrayList();
                        container.add(child.getValue());
                        fieldMap.put(split[split.length - 1], container);
                    } else {
                        //key存在的情况
                        container = new ArrayList();
                        container = fieldMap.get(split[split.length - 1]);
                        container.add(child.getValue());
                        fieldMap.put(split[split.length - 1], container);
                    }
                }
                //将map中的数据推到field里面
                fields[j].set(instance, fieldMap.get(split[split.length - 1]).size() == 1 ? fieldMap.get(split[split.length - 1]).get(0) : fieldMap.get(split[split.length - 1]));
                fields[j].setAccessible(false);
            } else {
                continue;
            }
        }
        return instance;
    }

    /***
     * editor: lzy
     * last modify: 2019-01-17
     * @param tag <mysqljdbc & hivejdbc & azkaban>
     * @param index find the element witch index equals user give
     * @return a list of jdbcConnectionArray for JDBCConnectionFactory to Create Connections
     */
    public static Object getXMInfo(String className, String filePath, String tag, int index) {
        //XMLBuilder
        SAXBuilder xmlBuilder = new SAXBuilder();
        Document doc = null;
        File file = null;
        Object instance = null;
        try {
            //读取xml文件
            file = new File(filePath);
            //文件不存在
            if (!file.exists()) {
                System.out.println("FILE IS NOT AVILIBLE");
            }
            doc = xmlBuilder.build(file);
            //拿到根元素
            Element rootElement = doc.getRootElement();
            //拿到我们需要的数据库连接类型类型
            List<Element> elementList = rootElement.getChildren(tag);
            //遍历拿到类型匹配的子节点
            for (int i = 0; i < elementList.size(); i++) {
                //添加index判断条件
                if (index == Integer.parseInt(elementList.get(i).getChild("comment").getAttributeValue("index"))) {
                    //通过反射创建对象实例
                    Class classname = Class.forName(className);
                    Constructor constructor = classname.getConstructor();
                    instance = constructor.newInstance();
                    //获取所有的成员变量
                    Field[] fields = classname.getDeclaredFields();
                    //加强版list复杂数据类型的封装
                    HashMap<Object, ArrayList> firldMap = new HashMap();
                    //循环获取成员变量的名称 split[length-1]
                    instance = encapsulationByReflex(instance, fields, firldMap, elementList.get(i));
                }
            }
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return instance;
        }
    }


    public static Map<String, String> getXMLInfo(String filePath, String tag, String[] childrenTagList){
        //XMLBuilder
        SAXBuilder xmlBuilder = new SAXBuilder();
        Document doc = null;
        File file = null;
        Object instance = null;
        HashMap<String, String> xmlInfoMap = new HashMap<String, String>();
        //读取xml文件
        file = new File(filePath);
        //文件不存在
        if (!file.exists()) {
            System.out.println("FILE IS NOT AVILIBLE");
        }
        try {
            doc = xmlBuilder.build(file);
            Element element = doc.getRootElement().getChildren(tag).get(0);
            for (String childrenTag : childrenTagList) {
                xmlInfoMap.put(childrenTag,element.getChildren(childrenTag).get(0).getValue());
            }
            for (String s : xmlInfoMap.keySet()) {
                System.out.println(s+" "+xmlInfoMap.get(s));
            }
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return xmlInfoMap;
    }
}
