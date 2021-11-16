package com.sugon.nebula.jgraph.schemaTools;

import com.google.common.collect.Lists;
import com.sugon.nebula.jgraph.tools.propertiesTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class schemaConfigBulider {


    private static final Logger LOGGER = LoggerFactory.getLogger(schemaConfigBulider.class);
    private String DBFILE = "db.properties";
    private Connection con;
    private static schemaConfigBulider dbConfiguration = new schemaConfigBulider();

    public static schemaConfigBulider get() {
        return dbConfiguration;
    }

    public static class nodePropertyDesc implements Serializable{
        public static final Long serialVersionUID = -987448824652078996L;
        private String tag;
        private List<nodeColDesc> cols;
        // 组合主键 通过这几个字段组合编码为节点id -> zjhm,xm,age
        private List<String> keyCols;


        public nodePropertyDesc(String tag, List<nodeColDesc> cols, List<String> keyCols) {
            this.tag = tag;
            this.cols = cols;
            this.keyCols = keyCols;
        }

        public static Long getSerialVersionUID() {
            return serialVersionUID;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public List<nodeColDesc> getCols() {
            return cols;
        }

        public void setCols(List<nodeColDesc> cols) {
            this.cols = cols;
        }

        public List<String> getKeyCols() {
            return keyCols;
        }

        public void setKeyCols(List<String> keyCols) {
            this.keyCols = keyCols;
        }
    }

    public List<String> getNodeIndexCols(String tag){
        nodePropertyDesc nodePropertyDesc = this.readNodePropertyDesc(tag);
        return nodePropertyDesc.cols.stream().filter(t -> t.isIndex)
                .map(t -> t.colName).collect(Collectors.toList());
    }


    public static class nodeColDesc implements Serializable{

        private String colName;
        private String colType;
        private boolean isKey = false;
        private boolean isStartKey = false;
        private boolean isendKey = false;
        private boolean isIndex = false;

        public boolean isStartKey() {
            return isStartKey;
        }

        public void setStartKey(boolean startKey) {
            isStartKey = startKey;
        }

        public boolean isIsendKey() {
            return isendKey;
        }

        public void setIsendKey(boolean isendKey) {
            this.isendKey = isendKey;
        }

        public boolean isKey() {
            return isKey;
        }

        public void setKey(boolean key) {
            isKey = key;
        }

        public nodeColDesc(String colName, String colType, boolean isKey, boolean isIndex){
            this.colName = colName;
            this.colType = colType;
            this.isKey = isKey;
            this.isIndex = isIndex;
        }

        public nodeColDesc(String colName, String colType){
            this.colName = colName;
            this.colType = colType;
        }

        public nodeColDesc(String colName, String colType,boolean isStartKey, boolean isendKey,boolean isIndex){
            this.colName = colName;
            this.colType = colType;
            this.isStartKey = isStartKey;
            this.isendKey = isendKey;
            this.isIndex = isIndex;
        }

        public String getColName() {
            return colName;
        }

        public void setColName(String colName) {
            this.colName = colName;
        }

        public String getColType() {
            return colType;
        }

        public boolean isIndex() {
            return isIndex;
        }

        public void setIndex(boolean index) {
            isIndex = index;
        }

        public void setColType(String colType) {
            this.colType = colType;
        }
    }


    public static class edgePropertyDesc implements Serializable{
        public static final Long serialVersionUID = -987448824652078996L;
        private String tag;
        private List<nodeColDesc> cols;
        // 组合主键 通过这几个字段组合编码为起始节点id -> start_zjhm,start_xm
        //跟本身节点的组成id字段一致
        private List<String> startKeyCols;
        // 组合主键 通过这几个字段组合编码为结束节点id -> end_zjhm,end_xm
        private List<String> endKeyCols;

        public edgePropertyDesc(String tag, List<nodeColDesc> cols, List<String> startKeyCols, List<String> endKeyCols) {
            this.tag = tag;
            this.cols = cols;
            this.startKeyCols = startKeyCols;
            this.endKeyCols = endKeyCols;
        }

        public static Long getSerialVersionUID() {
            return serialVersionUID;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public List<nodeColDesc> getCols() {
            return cols;
        }

        public void setCols(List<nodeColDesc> cols) {
            this.cols = cols;
        }
        public List<String> getStartKeyCols() {
            return startKeyCols;
        }

        public void setStartKeyCols(List<String> startKeyCols) {
            this.startKeyCols = startKeyCols;
        }

        public List<String> getEndKeyCols() {
            return endKeyCols;
        }

        public void setEndKeyCols(List<String> endKeyCols) {
            this.endKeyCols = endKeyCols;
        }

    }

    public synchronized Connection getConnection() {
        if (null != con) {
            return con;
        }
        try {
            Class.forName("com.mysql.jdbc.Driver");
            LOGGER.info("数据库驱动加载成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            String url = propertiesTool.getPropertyValue(DBFILE,"engline.mysql.jdbc-url");
            String username = propertiesTool.getPropertyValue(DBFILE,"engline.mysql.username");
            String password = propertiesTool.getPropertyValue(DBFILE,"engline.mysql.password");
            LOGGER.info("地址 ："+url);
            LOGGER.info("username ："+username);
            LOGGER.info("pwd ："+password);
            con = DriverManager.getConnection(url,username,password);
            LOGGER.info("数据库连接成功");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return con;
    }


    public nodePropertyDesc readNodePropertyDesc(String tag){
        String vertex_property_tab = propertiesTool.getPropertyValue(DBFILE,"VERTEX_PROPERTY_TAB");
        try {
            List<nodeColDesc> cols = Lists.newArrayList();
            List<String> keyCols = Lists.newArrayList();
            PreparedStatement statement = getConnection().prepareStatement("select type,prop_name,label ,is_master,is_index from " +
                    vertex_property_tab + " where label=?");
            statement.setString(1, tag);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                nodeColDesc n = new nodeColDesc(rs.getString("prop_name"),rs.getString("type"),
                        rs.getBoolean("is_master"),rs.getBoolean("is_index"));
                cols.add(n);
                if (n.isKey){
                    keyCols.add(n.getColName());
                }

            }
            return new nodePropertyDesc(tag,cols,keyCols);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public edgePropertyDesc readEdgePropertyDesc(String tag) {
        String edge_property_tab = propertiesTool.getPropertyValue(DBFILE,"EDGE_PROPERTY_TAB");
        try {
            List<nodeColDesc> cols = Lists.newArrayList();
            List<String> startKeyCols = Lists.newArrayList();
            List<String> endKeyCols = Lists.newArrayList();
            PreparedStatement statement = getConnection().prepareStatement("select type,prop_name,label ,is_start_vertex_key,is_end_vertex_key," +
                            "is_index from " + edge_property_tab + " where label=?");
            statement.setString(1, tag);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                nodeColDesc n = new nodeColDesc(rs.getString("prop_name"),rs.getString("type"),
                        rs.getBoolean("is_start_vertex_key"),rs.getBoolean("is_end_vertex_key"),
                        rs.getBoolean("is_index"));
                cols.add(n);
                if (n.isStartKey){
                    startKeyCols.add(n.getColName());
                }
                if (n.isendKey){
                    endKeyCols.add(n.getColName());
                }

            }
            return new edgePropertyDesc(tag,cols,startKeyCols,endKeyCols);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        List<String> person = schemaConfigBulider.get().getNodeIndexCols("person");
        System.out.println(person);
    }

}
