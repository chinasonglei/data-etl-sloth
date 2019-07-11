package com.beadwallet.bean;

import java.io.File;
import java.util.List;

public class AutoGenerationInfo {
    //所在文件夹
    private File folder;
    //zip文件
    private File zipFile;
    //Flow文件
    private List<File> flowFileList;

    public File getFolder() {
        return folder;
    }

    public void setFolder(File folder) {
        this.folder = folder;
    }

    public File getZipFile() {
        return zipFile;
    }

    public void setZipFile(File zipFile) {
        this.zipFile = zipFile;
    }

    public List<File> getFlowFileList() {
        return flowFileList;
    }

    public void setFlowFileList(List<File> flowFileList) {
        this.flowFileList = flowFileList;
    }

    public String toString() {
        return "folder=" + folder
            + " ,zipFile=" + zipFile
            + " ,flowFileList" + flowFileList.toString() + "\n";
    }
}
