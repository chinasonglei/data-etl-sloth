package com.beadwallet.utils.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件处理通用类
 *
 * @author QuChunhui 2019/01/28
 */
public class FileUtil {

    /**
     * 获取指定路径下的所有子文件夹
     * @param path 路径
     * @return 子文件夹列表
     */
    public static List<File> getChildFolderList(String path) {
        File[] allFiles = new File(path).listFiles();
        List<File> folderList = new ArrayList<>();
        if (allFiles != null) {
            for (File file : allFiles) {
                if (file.isDirectory()) {
                    folderList.add(file);
                }
            }
        }
        return folderList;
    }

    /**
     * 获取指定路径下，指定扩展名的文件列表
     * @param fileList [OUT]文件列表
     * @param path [IN]指定路径
     * @param extName [IN]指定扩展名
     */
    public static void getFileList(List<File> fileList, String path, String extName) {
        File[] allFiles = new File(path).listFiles();
        if (allFiles != null) {
            for (File file : allFiles) {
                if (file.isFile()) {
                    String fileName = file.getName();
                    if (fileName.endsWith(extName)) {
                        fileList.add(file);
                    }
                } else {
                    getFileList(fileList, file.getAbsolutePath(), extName);
                }
            }
        }
    }
}
