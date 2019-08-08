package com.gmg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author gmg
 * @title: Demo
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/7 9:59
 */
public class Demo {

    Configuration conf = null;
    Connection conn = null;

    /**
     * 连接
     */
    public void getConfigAndConnection() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata01,bigdata02,bigdata03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @throws IOException
     */
    public void createTable() throws IOException {
        Admin admin = conn.getAdmin();
        if(!admin.isTableAvailable(TableName.valueOf("test"))) {
            TableName tableName = TableName.valueOf("test");
            //表描述器构造器
            TableDescriptorBuilder tdb  =TableDescriptorBuilder.newBuilder(tableName)  ;
            //列族描述起构造器
            ColumnFamilyDescriptorBuilder cdb =  ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));
            //获得列描述起
            ColumnFamilyDescriptor  cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            //admin.addColumnFamily(tableName, cfd); //给标添加列族
            admin.createTable(td);
        }
        //关闭链接
    }

    /**
     * 单条插入
     * @throws IOException
     */
    public void insertOneData() throws IOException {
        //new 一个列  ，hgs_000为row key
        Put put = new Put(Bytes.toBytes("hgs_000"));
        //下面三个分别为，列族，列名，列值
        put.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("name") , Bytes.toBytes("hgs"));
        TableName tableName = TableName.valueOf("test");
        //得到 table
        Table table = conn.getTable(tableName);
        //执行插入
        table.put(put);
    }

    /**
     * 插入多个列
     * @throws IOException
     */
    public void insertManyData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("hgs_001"));
        put1.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("name") , Bytes.toBytes("wd"));

        Put put2 = new Put(Bytes.toBytes("hgs_001"));
        put2.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("age") , Bytes.toBytes("25"));

        Put put3 = new Put(Bytes.toBytes("hgs_001"));
        put3.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("weight") , Bytes.toBytes("60kg"));

        Put put4 = new Put(Bytes.toBytes("hgs_001"));
        put4.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("sex") , Bytes.toBytes("男"));
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);
        table.close();
    }

    /**
     * 同一条数据的插入
     * @throws IOException
     */
    public void singleRowInsert() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));

        Put put1 = new Put(Bytes.toBytes("hgs_005"));

        put1.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("name") , Bytes.toBytes("cm"));
        put1.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("age") , Bytes.toBytes("22"));
        put1.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("weight") , Bytes.toBytes("88kg"));
        put1.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("sex") , Bytes.toBytes("男"));

        table.put(put1);
        table.close();
    }

    /**
     * 数据的更新,hbase对数据只有追加，没有更新，但是查询的时候会把最新的数据返回给哦我们
     * @throws IOException
     */
    public void updateData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        Put put1 = new Put(Bytes.toBytes("hgs_002"));
        put1.addColumn(Bytes.toBytes("testfm"),Bytes.toBytes("weight") , Bytes.toBytes("63kg"));
        table.put(put1);
        table.close();
    }

    /**
     *     删除数据
     * @throws IOException
     */
    public void deleteData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        //参数为 row key
        //删除一列
        Delete delete1 = new Delete(Bytes.toBytes("hgs_000"));
        delete1.addColumn(Bytes.toBytes("testfm"), Bytes.toBytes("weight"));
        //删除多列
        Delete delete2 = new Delete(Bytes.toBytes("hgs_001"));
        delete2.addColumns(Bytes.toBytes("testfm"), Bytes.toBytes("age"));
        delete2.addColumns(Bytes.toBytes("testfm"), Bytes.toBytes("sex"));
        //删除某一行的列族内容
        Delete delete3 = new Delete(Bytes.toBytes("hgs_002"));
        delete3.addFamily(Bytes.toBytes("testfm"));

        //删除一整行
        Delete delete4 = new Delete(Bytes.toBytes("hgs_003"));
        table.delete(delete1);
        table.delete(delete2);
        table.delete(delete3);
        table.delete(delete4);
        table.close();
    }

    /**
     * 查询
     * @throws IOException
     */
    public void querySingleRow() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        //获得一行
        Get get = new Get(Bytes.toBytes("hgs_000"));
        Result set = table.get(get);
        Cell[] cells  = set.rawCells();
        for(Cell cell : cells) {
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        table.close();
        //Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password")))

    }

    /**
     * 全表扫描
     * @throws IOException
     */
    public void scanTable() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        Scan scan = new Scan();
        ResultScanner rsacn = table.getScanner(scan);
        for(Result rs:rsacn) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 列值过滤器
     * @throws IOException
     */
    public void singColumnFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        Scan scan = new Scan();
        //下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter =  new SingleColumnValueFilter( Bytes.toBytes("testfm"),  Bytes.toBytes("name"),
                CompareOperator.EQUAL,  Bytes.toBytes("wd")) ;
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * row key过滤器
     * @throws IOException
     */
    public void rowkeyFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareOperator.EQUAL,new RegexStringComparator("^hgs_00*"));
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 列名前缀过滤器
     * @throws IOException
     */
    public void columnPrefixFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 过滤器集合
     * @throws IOException
     */
    public void FilterSet() throws IOException {
        Table table = conn.getTable(TableName.valueOf("test"));
        Scan scan = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 =  new SingleColumnValueFilter( Bytes.toBytes("testfm"),  Bytes.toBytes("age"),
                CompareOperator.GREATER,  Bytes.toBytes("23")) ;
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("weig"));
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }

    }

    public void closeConn() throws IOException {
        conn.close();
    }
    public static void main(String[] args) {

    }
}
