package com.beadwallet.utils.xml;

import java.lang.reflect.InvocationTargetException;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * XMLReader工具类
 *
 * @author QuChunhui 2019/01/28
 */
public class XMLReader {
    private static final Logger logger = LoggerFactory.getLogger(XMLReader.class);

    /***
     * 解析XML，并转换为Java对象
     *
     * @param tag XML标签名
     * @param index 标签Index
     * @return Object instance
     */
    public static Object getXMInfo(String className, String filePath, String tag, int index) {
        //XMLBuilder
        SAXBuilder xmlBuilder = new SAXBuilder();
        Object instance = null;

        try {
            //读取xml文件
            File file = new File(filePath);

            //文件不存在
            if (!file.exists()) {
                logger.error("XML file is not exist.");
                return null;
            }

            //拿到Document对象
            Document doc = xmlBuilder.build(file);

            //拿到根元素
            Element rootElement = doc.getRootElement();

            //拿到我们需要的数据库连接类型类型
            List<Element> elementList = rootElement.getChildren(tag);

            //遍历拿到类型匹配的子节点
            for (Element element : elementList) {
                //添加comment判断条件
                Element comment = element.getChild("comment");
                if (comment == null) {
                    continue;
                }

                //添加index判断条件
                String indexStr = comment.getAttributeValue("index");
                if (index != Integer.parseInt(indexStr)) {
                    continue;
                }

                //通过反射创建对象实例
                Class<?> classname = Class.forName(className);
                Constructor constructor = classname.getConstructor();
                instance = constructor.newInstance();

                //获取所有的成员变量
                Field[] fields = classname.getDeclaredFields();

                //加强版list复杂数据类型的封装
                HashMap<Object, ArrayList<String>> fieldMap = new HashMap<>();

                //循环获取成员变量的名称 split[length-1]
                for (Field field : fields) {
                    //私有成员变量开放操作权限
                    field.setAccessible(true);

                    //拿到具体的对象名
                    String[] split = field.toString().split("\\.");
                    String fieldName = split[split.length - 1];

                    //拿到对象名对应的xml信息,封装到map<key,Array>里
                    List<Element> children = element.getChildren(fieldName);
                    if (children.size() < 1) {
                        continue;
                    }

                    //获取XML属性的值
                    for (Element child : children) {
                        if (!fieldMap.containsKey(fieldName)) {
                            fieldMap.put(fieldName, new ArrayList<>());
                        }
                        fieldMap.get(fieldName).add(child.getValue());
                    }

                    //将map中的数据推到field里面
                    if (fieldMap.get(fieldName).size() == 1) {
                        field.set(instance, fieldMap.get(fieldName).get(0));
                    } else {
                        field.set(instance, fieldMap.get(fieldName));
                    }
                    field.setAccessible(false);
                }
            }
        } catch (JDOMException | IOException | InstantiationException | InvocationTargetException
            | NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
            logger.error("xml reader exception." + e.getMessage());
            return null;
        }

        return instance;
    }
}
