package com.iotek;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by x on 2023/4/12.
 */
public class Hdfs {

    private  static FileSystem fs;
    static {
        Configuration configuration=new Configuration() ;
        try {
            fs=FileSystem.get(new URI("hdfs://192.168.47.128:9000"),configuration,"hadoop");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    //上传hdfs服务器
    public static  void uploadFile(String localpath,String hdfspath) throws IOException {

        fs.copyFromLocalFile(new Path(localpath),new Path(hdfspath));
    }

    //下载到本地
    public static  void downloadFile(String hdfspath,String localpath) throws IOException {
      // fs.copyToLocalFile(new Path(hdfspath),new Path(localpath));该方法会抛空指针
        fs.copyToLocalFile(false,new Path(hdfspath),new Path(localpath),true);
    }


    public static  void mkDir(String hdfspath) throws IOException {
        fs.mkdirs(new Path(hdfspath));
    }

    public static  void removeDir(String hdfspath) throws IOException {
        fs.delete(new Path(hdfspath),true);
    }


    public static  void listFile(String hdfspath) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfspath));
        for (FileStatus f:fileStatuses){
            String name = f.getPath().getName();
            if(f.isDirectory()){
                System.out.println(name+"  is derectory");
            }else{
                System.out.println(name+"  is file");
            }


        }
    }
    public static  void listDetail(String hdfspath) throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path(hdfspath), true);

       while (locatedFileStatusRemoteIterator.hasNext()){
           LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
           String owner = next.getOwner();
           System.out.println(owner);
           short replication = next.getReplication();
           System.out.println("副本数："+replication);
       }



    }


    public static void main(String[] args) throws IOException {

      // uploadFile("C:\\Users\\x\\Desktop\\文档.txt","/input");
       downloadFile("/output/result","C:\\output\\wordcount.txt");
      //  mkDir("/input/bigdata");
     //  removeDir("/myfloder/test");
     //listFile("/input");
      //  listDetail("/input");

    }




}


