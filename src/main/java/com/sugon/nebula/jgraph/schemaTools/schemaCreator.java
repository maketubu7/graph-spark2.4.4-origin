package com.sugon.nebula.jgraph.schemaTools;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sugon.nebula.jgraph.tools.BaseNebulaTool;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class schemaCreator {


    private static final Logger LOGGER = LoggerFactory.getLogger(schemaCreator.class);
    private static final String CREATE_SPACE_FORMAT = "CREATE SPACE IF EXISTS %s (partition_num = 2,replica_factor = 1,vid_type = FIXED_STRING(128))";
    private static final String CREATE_TAG_FORMAT = "CREATE TAG IF NOT EXISTS %s(%s)";
    private static final String CREATE_EDGE_FORMAT = "CREATE EDGE IF NOT EXISTS %s(%s)";
    private static final String CREATE_STRING_INDEX_FORMAT = "CREATE TAG INDEX %s on %s(%s(10));";
    private static final String CREATE_INT_INDEX_FORMAT = "CREATE TAG INDEX %s on %s(%s);";



    public static void createGraphspace(String spaceName, NebulaPool pool){
        try {
            Session conn = BaseNebulaTool.getConn(pool);
            conn.execute(String.format(CREATE_SPACE_FORMAT, spaceName));
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static boolean existsGraphspace(String spaceName, NebulaPool pool){
        boolean result = false;
        try {
            Session conn = BaseNebulaTool.getConn(pool);
            ResultSet resp = conn.execute("SHOW SPACES");
            for (int i = 0; i<resp.rowsSize();i++){
                List<ValueWrapper> values = resp.rowValues(i).values();
                for (ValueWrapper v : values){
                    if (v.asString().equals(spaceName)){
                        result =true;
                    }
                }
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public static boolean existsGraphTag(String spaceName, String tagName,NebulaPool pool){
        boolean result = false;
        try {
            Session conn = BaseNebulaTool.getConn(pool);
            ResultSet resp = conn.execute(String.format("USE %s;SHOW TAGS", spaceName));
            for (int i = 0; i<resp.rowsSize();i++){
                List<ValueWrapper> values = resp.rowValues(i).values();
                for (ValueWrapper v : values){
                    if (v.asString().equals(tagName)){
                        result =true;
                    }
                }
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public static boolean existsGraphEdge(String spaceName, String EdgeName,NebulaPool pool){
        boolean result = false;
        try {
            Session conn = BaseNebulaTool.getConn(pool);
            ResultSet resp = conn.execute(String.format("USE %s;SHOW EDGES", spaceName));
            for (int i = 0; i<resp.rowsSize();i++){
                List<ValueWrapper> values = resp.rowValues(i).values();
                for (ValueWrapper v : values){
                    if (v.asString().equals(EdgeName)){
                        result =true;
                    }
                }
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public static boolean existsGraphTagIndex(NebulaPool pool,String spaceName, String indexName,String indexType){
        boolean result = false;
        String executeStr = "";
        try {
            Session conn = BaseNebulaTool.getConn(pool);
            if ("tag".equals(indexType)){
                executeStr = String.format("USE %s;SHOW TAG INDEXES", spaceName);
            }
            else{
                executeStr = String.format("USE %s;SHOW EDGE INDEXES", spaceName);
            }

            ResultSet resp = conn.execute(executeStr);
            for (int i = 0; i<resp.rowsSize();i++){
                List<ValueWrapper> values = resp.rowValues(i).values();
                for (ValueWrapper v : values){
                    if (v.asString().equals(indexName)){
                        return true;
                    }
                }
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }



    /**
     *
     * @param pool
     * @param spaceName spaceName -> graphName
     * @param tagName  TAG名称 -> person
     * @param tagCols  TAG属性名称及类型 -> zjhm string,xm string,age int8
     */
    public static boolean createGraphTag(NebulaPool pool,String spaceName,String tagName,String tagCols){
        boolean result = false;
        Session conn = BaseNebulaTool.getConn(pool);
        try {
            if (!existsGraphspace(spaceName,pool)){
                createGraphspace(spaceName,pool);
            }
            conn.execute("use " + spaceName);
            ResultSet resp = conn.execute(String.format(CREATE_TAG_FORMAT, tagName, tagCols));
            LOGGER.info("execute: " + String.format(CREATE_TAG_FORMAT, tagName, tagCols));
            if(resp.isSucceeded()){
                result = true;
            }
            conn.release();
        }catch (Exception e){
            conn.release();
            e.printStackTrace();
        }
        return result;
    }

    /**
     *
     * @param pool
     * @param spaceName spaceName -> graphName
     * @param edgeName EDGE名称 -> person
     * @param edgeCols EDGE属性名称及类型 -> start_zjhm string,end_zjhm string,start_time int,end_time int
     * @return true
     */
    public static boolean createGraphEdge(NebulaPool pool,String spaceName,String edgeName,String edgeCols){
        boolean result = false;
        Session conn = BaseNebulaTool.getConn(pool);
        try {
            if (!existsGraphspace(spaceName,pool)){
                createGraphspace(spaceName,pool);
            }
            conn.execute("use " + spaceName);
            ResultSet resp = conn.execute(String.format(CREATE_EDGE_FORMAT, edgeName, edgeCols));
            LOGGER.info("execute: " + String.format(CREATE_EDGE_FORMAT, edgeName, edgeCols));
            if(resp.isSucceeded()){
                result = true;
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 创建单字段索引
     * @param pool
     * @param spaceName
     * @param tagName
     * @param indexCol
     * @return
     */
    public static String createIndex(NebulaPool pool, String spaceName, String tagName, schemaConfigBulider.nodeColDesc indexCol){
        Session conn = BaseNebulaTool.getConn(pool);
        String executeStr = "";
        String indexName = "";
        try {
            assert existsGraphTag(spaceName,tagName,pool): "tag not exists";
            conn.execute("use " + spaceName);
            indexName = spaceName + "_" + tagName + "_" + indexCol.getColName();
            if ("string".equals(indexCol.getColType())){
                executeStr = String.format(CREATE_STRING_INDEX_FORMAT, indexName, tagName,indexCol.getColName());
            }
            if ("int".equals(indexCol.getColType())){
                executeStr = String.format(CREATE_INT_INDEX_FORMAT, indexName, tagName,indexCol.getColName());
            }
            ResultSet resp = conn.execute(executeStr);
            LOGGER.info("execute: " + executeStr);
            if(!resp.isSucceeded()){
                indexName = "";
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return indexName;
    }

    public static boolean rebuildIndex(NebulaPool pool, String spaceName, String indexName, String indexType){
        boolean result = false;
        Session conn = BaseNebulaTool.getConn(pool);
        String executeStr = "";
        try {
            assert existsGraphTagIndex(pool,spaceName,indexName,indexType): "index not exists";
            conn.execute("use " + spaceName);

            if ("tag".equals(indexType)){
                executeStr = String.format("REBUILD TAG INDEX %s", indexName);
            }else {
                executeStr = String.format("REBUILD EDGE INDEX %s", indexName);
            }
            ResultSet resp = conn.execute(executeStr);
            LOGGER.info("execute: " + executeStr);
            if(resp.isSucceeded()){
                result = true;
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    /**
     *  根据某索引字段返回node信息
     * @param pool
     * @param spaceName
     * @param tagName
     * @param colName
     * @param colValue
     * @return
     */
    public static List<Node> getVertex(NebulaPool pool, String spaceName, String tagName, String colName, String colValue){
        Session conn = BaseNebulaTool.getConn(pool);
        List<Node> result = new ArrayList<>();
        try{
            conn.execute("use " + spaceName);
            ResultSet resp = conn.execute(String.format("MATCH (v:%s{%s:\"%s\"}) RETURN v;",tagName, colName,colValue));
            if (resp.isSucceeded()){
                for (int i = 0; i<resp.rowsSize();i++){
                    List<ValueWrapper> values = resp.rowValues(i).values();
                    for (ValueWrapper v : values){
                        result.add(v.asNode());
                    }
                }
            }
            System.out.println(result);
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public static String getVertexVid(NebulaPool pool, String spaceName, String tagName, String colName, String colValue){
        Session conn = BaseNebulaTool.getConn(pool);
        String vid = "";
        List<Node> result = new ArrayList<>();
        try{
            if (!existsGraphspace(spaceName,pool) || !existsGraphTag(spaceName,tagName,pool)){
                LOGGER.error("该图空间或者TAG不存在");
                return vid;
            }
            conn.execute("use " + spaceName);
            ResultSet resp = conn.execute(String.format("MATCH (v:%s{%s:\"%s\"}) RETURN v;",tagName, colName,colValue));
            if (resp.isSucceeded()){
                for (int i = 0; i<resp.rowsSize();i++){
                    List<ValueWrapper> values = resp.rowValues(i).values();
                    for (ValueWrapper v : values){
                        result.add(v.asNode());
                    }
                }
            }
            if (result.size() == 1){
                vid = result.get(0).getId().asString();
            }
            conn.release();
        }catch (Exception e){
            e.printStackTrace();
        }
        return vid;
    }


    public static String formatVertex(List<Node> result) throws UnsupportedEncodingException {
        JSONArray jsonArray = new JSONArray();
        for (Node n: result){
            JSONObject object = new JSONObject();
            String vid = n.getId().asString();
            object.put("vid",vid);
            // 默认每个点都只有一个tag
            HashMap<String, ValueWrapper> properties = n.properties(n.tagNames().get(0));
            for (String key:properties.keySet()){
                ValueWrapper value = properties.get(key);
                if (value.isLong()){
                    object.put(key, value.asLong());
                }else {
                    object.put(key, value.asString());
                }
            }
            jsonArray.add(object);
        }
        return jsonArray.toString();
    }


    public static void main(String[] args) throws UnsupportedEncodingException {
        NebulaPool pool = BaseNebulaTool.initPool();
        String graphspace = "graphspace_test";
        String tag = "person";
        String edgeType = "follow";

        schemaConfigBulider.nodeColDesc indexDesc = new schemaConfigBulider.nodeColDesc("ysz","string",true,true);
        String b = createIndex(pool, graphspace, tag, indexDesc);

        rebuildIndex(pool,graphspace,b,"tag");

        pool.close();
    }


}
