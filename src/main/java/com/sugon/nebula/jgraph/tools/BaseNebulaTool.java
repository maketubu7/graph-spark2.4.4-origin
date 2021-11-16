package com.sugon.nebula.jgraph.tools;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseNebulaTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseNebulaTool.class);
    private static final NebulaPool pool = new NebulaPool();

    public synchronized static NebulaPool initPool(){

        try{
            NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
            nebulaPoolConfig.setMaxConnSize(10);
            String graphdhosts = propertiesTool.getPropertyValue("nebula.properties","nebula.graphd.hosts");
            String graphdport = propertiesTool.getPropertyValue("nebula.properties","nebula.graphd.port");
            String[] hosts = graphdhosts.split(",");
            List<HostAddress> addresses = new ArrayList<>();
            for (String host: hosts){
                addresses.add(new HostAddress(host, Integer.parseInt(graphdport)));
            }
            pool.init(addresses, nebulaPoolConfig);
            LOGGER.info("nebula连接池创建成功");
        }catch (Exception e){
            e.printStackTrace();
        }
        return pool;
    }

    public static Session getConn(NebulaPool pool){
        Session session = null;
        try{
            session = pool.getSession("root", "nebula", false);
        }catch (Exception e){
            e.printStackTrace();
        }
        LOGGER.info("创建连接");
        return session;
    }




    public static void main(String[] args) throws IOErrorException {
        NebulaPool nebulaPool = initPool();
        Session conn = getConn(nebulaPool);
        conn.execute("use graphspace_test;");
        ResultSet resp = conn.execute("FETCH PROP ON person \"person001\";");
        System.out.println(resp);
    }
}
